package io.tofhir.integrationtest

import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.path.FhirPathUtilFunctionsFactory
import io.tofhir.{OnFhirTestContainer, ToFhirTestSpec}
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.mapping.job.FhirMappingJobManager
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingUtility
import org.apache.commons.io
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.streaming.StreamingQuery
import org.rnorth.ducttape.unreliables.Unreliables
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Assertion, BeforeAndAfterAll}
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.io.File
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties, UUID}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

class KafkaSourceIntegrationTest extends AnyFlatSpec with ToFhirTestSpec with BeforeAndAfterAll with OnFhirTestContainer {

  override protected def afterAll(): Unit = {
    deleteResources()
    if (adminClient != null) adminClient.close()
    if (producer != null) producer.close()
    if (consumer != null) consumer.close()
    kafka.close()
    super.afterAll()
  }

  private def deleteResources(): Assertion = {
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
    batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p1")))
    val f = onFhirClient.search("Observation").where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p1"))
      .executeAndReturnBundle() flatMap { obsBundle =>
      obsBundle.searchResults.foreach(obs =>
        batchRequest = batchRequest.entry(_.delete("Observation", (obs \ "id").extract[String]))
      )
      batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
        res.httpStatus shouldBe StatusCodes.OK
      }
    }
    Await.result(f, FiniteDuration(60, TimeUnit.SECONDS))
  }

  //kafka test-containers related
  val kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
  kafka.start()
  val kafkaPort: Integer = kafka.getFirstMappedPort
  val bootstrapServers: String = kafka.getBootstrapServers

  val bootstrapProperties = new Properties
  bootstrapProperties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  val adminClient: AdminClient = AdminClient.create(bootstrapProperties)

  val producerProperties = new Properties
  val producerMap: Map[String, String] = Map(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers, ProducerConfig.CLIENT_ID_CONFIG -> UUID.randomUUID.toString)
  producerMap.foreach { case (key, value) => producerProperties.setProperty(key, value) }

  val consumerProperties = new Properties
  val consumerMap: Map[String, String] = Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers, ConsumerConfig.GROUP_ID_CONFIG -> "tc-".+(UUID.randomUUID.toString), ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
  consumerMap.foreach { case (key, value) => consumerProperties.setProperty(key, value) }

  val producer = new KafkaProducer[String, String](producerProperties, new StringSerializer, new StringSerializer)
  val consumer = new KafkaConsumer[String, String](consumerProperties, new StringDeserializer, new StringDeserializer)

  val streamingSourceSettings: Map[String, KafkaSourceSettings] =
    Map("kafka-source" -> KafkaSourceSettings("kafka-source", "https://aiccelerate.eu/data-integration-suite/kafka-data", s"PLAINTEXT://localhost:$kafkaPort"))

  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = onFhirClient.getBaseUrl())

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    name = "patient-mapping",
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceBinding = Map("source" -> KafkaSource(topicName = "patients", sourceRef = Some("kafka-source"), options = Map("startingOffsets" -> "earliest")))
  )
  val otherObservationMappingTask: FhirMappingTask = FhirMappingTask(
    name = "other-observation-mapping",
    mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-mapping",
    sourceBinding = Map("source" -> KafkaSource(topicName = "observations", sourceRef = Some("kafka-source"), options = Map("startingOffsets" -> "earliest")))
  )
  val familyMemberHistoryMappingTask: FhirMappingTask = FhirMappingTask(
    name = "family-member-history-mapping",
    mappingRef = "https://aiccelerate.eu/fhir/mappings/family-member-history-mapping",
    sourceBinding = Map("source" -> KafkaSource(topicName = "familyMembers", sourceRef = Some("kafka-source"), options = Map("startingOffsets" -> "earliest")))
  )

  val fhirMappingJob: FhirMappingJob = FhirMappingJob(
    name = Some("test-job"),
    mappings = Seq.empty,
    sourceSettings = streamingSourceSettings,
    sinkSettings = fhirSinkSettings,
    dataProcessingSettings = DataProcessingSettings()
  )

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, Map(FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory), sparkSession)

  it should "check the test container working" in {
    val topicName = "testTopic"
    val topics = Collections.singletonList(new NewTopic(topicName, 1, 1.toShort))
    adminClient.createTopics(topics).all.get(30, TimeUnit.SECONDS)
    consumer.subscribe(Collections.singletonList(topicName))
    producer.send(new ProducerRecord[String, String](topicName, "testContainer-key", "testContainer-value")).get
    Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () => {
      def foo(): Boolean = {
        val records = consumer.poll(Duration.ofMillis(100))
        if (records.isEmpty) return false
        records.iterator.next.topic shouldBe "testTopic"
        records.iterator.next.key shouldBe "testContainer-key"
        records.iterator.next.value shouldBe "testContainer-value"
        true
      }
      foo()
    })
    consumer.unsubscribe()
  }

  it should "produce data to patients topic" in {
    val topicName = "patients"
    val topics = Collections.singletonList(new NewTopic(topicName, 1, 1.toShort))
    adminClient.createTopics(topics).all.get(30, TimeUnit.SECONDS)
    consumer.subscribe(Collections.singletonList(topicName))
    producer.send(new ProducerRecord[String, String](topicName, "1", "{\"pid\":\"p1\",\"gender\":\"male\",\"birthDate\":\"1995-11-10\"}")).get
    Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () => {
      def foo(): Boolean = {
        val records = consumer.poll(Duration.ofMillis(100))
        if (records.isEmpty) return false
        records.iterator.next.topic shouldBe "patients"
        records.iterator.next.key shouldBe "1"
        records.iterator.next.value shouldBe "{\"pid\":\"p1\",\"gender\":\"male\",\"birthDate\":\"1995-11-10\"}"
        true
      }
      foo()
    })
    consumer.unsubscribe()
  }

  it should "produce data to observations topic" in {
    val topicName = "observations"
    val topics = Collections.singletonList(new NewTopic(topicName, 1, 1.toShort))
    adminClient.createTopics(topics).all.get(30, TimeUnit.SECONDS)
    consumer.subscribe(Collections.singletonList(topicName))
    producer.send(new ProducerRecord[String, String](topicName, "1", "{\"pid\":\"p1\",\"time\":\"2007-10-12 10:00:00\",\"encounterId\":\"e1\",\"code\":\"9110-8\",\"value\":\"450\"}")).get
    Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () => {
      def foo(): Boolean = {
        val records = consumer.poll(Duration.ofMillis(100))
        if (records.isEmpty) return false
        records.iterator.next.topic shouldBe "observations"
        records.iterator.next.key shouldBe "1"
        records.iterator.next.value shouldBe "{\"pid\":\"p1\",\"time\":\"2007-10-12 10:00:00\",\"encounterId\":\"e1\",\"code\":\"9110-8\",\"value\":\"450\"}"
        true
      }
      foo()
    })
    consumer.unsubscribe()
  }

  it should "produce data to familyMembers topic" in {
    val topicName = "familyMembers"
    val message = "{\"name\":\"test\",\"deceased\":\"true\",\"birthDate_y\":\"1995\",\"birthDate_m\":\"04\",\"birthDate_d\":\"12\"}"
    val topics = Collections.singletonList(new NewTopic(topicName, 1, 1.toShort))
    adminClient.createTopics(topics).all.get(30, TimeUnit.SECONDS)
    consumer.subscribe(Collections.singletonList(topicName))
    producer.send(new ProducerRecord[String, String](topicName, "1", message)).get
    Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () => {
      def foo(): Boolean = {
        val records = consumer.poll(Duration.ofMillis(100))
        if (records.isEmpty) return false
        records.iterator.next.topic shouldBe "familyMembers"
        records.iterator.next.key shouldBe "1"
        records.iterator.next.value shouldBe message
        true
      }
      foo()
    })
    consumer.unsubscribe()
  }

  it should "consume patients, observations and family member history data and map and write to the fhir repository" in {
    val execution: FhirMappingJobExecution = FhirMappingJobExecution(job = fhirMappingJob, mappingTasks = Seq(patientMappingTask, otherObservationMappingTask, familyMemberHistoryMappingTask))
    val streamingQueryFutures: Map[String, Future[StreamingQuery]] = fhirMappingJobManager.startMappingJobStream(mappingJobExecution =
      execution,
      sourceSettings = streamingSourceSettings,
      sinkSettings = fhirSinkSettings
    )
    streamingQueryFutures.foreach(sq => {
      val streamingQuery: StreamingQuery = Await.result(sq._2, FiniteDuration.apply(60, TimeUnit.SECONDS)) // First wait for the StreamingQuery to become available
      streamingQuery.awaitTermination(20000L) // Wait for 20 seconds to consume and write to the fhir repo and terminate
      streamingQuery.stop()
      io.FileUtils.deleteDirectory(new File(execution.getCheckpointDirectory(sq._1))) // Clear checkpoint directory to prevent conflicts with other tests
    })

    val searchTest = onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p1")).executeAndReturnResource() flatMap { p1Resource =>
      FHIRUtil.extractIdFromResource(p1Resource) shouldBe FhirMappingUtility.getHashedId("Patient", "p1")
      FHIRUtil.extractValue[String](p1Resource, "gender") shouldBe "male"
      FHIRUtil.extractValue[String](p1Resource, "birthDate") shouldBe "1995-11-10"

      onFhirClient.search("Observation").where("code", "9110-8").executeAndReturnBundle() flatMap  { observationBundle =>
        (observationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
          FhirMappingUtility.getHashedReference("Patient", "p1")

        onFhirClient.read("FamilyMemberHistory", FhirMappingUtility.getHashedId("FamilyMemberHistory","test")).executeAndReturnResource() map {resource =>
          FHIRUtil.extractIdFromResource(resource) shouldBe FhirMappingUtility.getHashedId("FamilyMemberHistory", "test")
          FHIRUtil.extractValue[Boolean](resource, "deceasedBoolean") shouldBe true
          FHIRUtil.extractValue[String](resource, "bornDate") shouldBe "1995-04-12"
        }
      }
    }
    Await.result(searchTest, FiniteDuration(60, TimeUnit.SECONDS))
  }

  it should "should not throw an exception when it encounters a corrupted topic message" in {
    // publish a corrupted message to the familyMembersCorrupted topic
    val topicName = "familyMembersCorrupted"
    val topics = Collections.singletonList(new NewTopic(topicName, 1, 1.toShort))
    adminClient.createTopics(topics).all.get(30, TimeUnit.SECONDS)
    consumer.subscribe(Collections.singletonList(topicName))
    producer.send(new ProducerRecord[String, String](topicName, "1", "{\"name\":\"test\",\"deceased\":\"true\",\"birthDate_y\":\"1995\",")).get
    Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () => {
      def foo(): Boolean = {
        val records = consumer.poll(Duration.ofMillis(100))
        if (records.isEmpty) return false
        records.iterator.next.topic shouldBe "familyMembersCorrupted"
        records.iterator.next.key shouldBe "1"
        records.iterator.next.value shouldBe "{\"name\":\"test\",\"deceased\":\"true\",\"birthDate_y\":\"1995\","
        true
      }
      foo()
    })
    consumer.unsubscribe()
    // modify familyMemberHistoryMappingTask to listen to familyMembersCorrupted topic
    val mappingTask = familyMemberHistoryMappingTask.copy(sourceBinding = Map("source" -> KafkaSource(topicName = "familyMembersCorrupted", options = Map("startingOffsets" -> "earliest"))))
    val execution: FhirMappingJobExecution = FhirMappingJobExecution(job = fhirMappingJob, mappingTasks = Seq(mappingTask))
    val streamingQueryFutures: Map[String, Future[StreamingQuery]] = fhirMappingJobManager.startMappingJobStream(
      mappingJobExecution = execution,
      sourceSettings = streamingSourceSettings,
      sinkSettings = fhirSinkSettings)
    val streamingQuery = Await.result(streamingQueryFutures.head._2, FiniteDuration(5, TimeUnit.SECONDS))

    streamingQuery.awaitTermination(20000L)
    streamingQuery.stop()
    io.FileUtils.deleteDirectory(new File(execution.getCheckpointDirectory(mappingTask.name))) // Clear checkpoint directory to prevent conflicts with other tests
  }
}

