package io.onfhir.tofhir.engine

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.util.FHIRUtil
import io.onfhir.client.OnFhirNetworkClient
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.ErrorHandlingType
import io.onfhir.tofhir.engine.Execution.actorSystem.dispatcher
import io.onfhir.tofhir.model._
import io.onfhir.tofhir.util.FhirMappingUtility
import io.onfhir.util.JsonFormatter.formats
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.sql.streaming.StreamingQuery
import org.rnorth.ducttape.unreliables.Unreliables
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Assertion, BeforeAndAfterAll, Ignore}
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties, UUID}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future, duration}
import scala.util.Try

@Ignore
class KafkaSourceIntegrationTest extends AnyFlatSpec with ToFhirTestSpec with BeforeAndAfterAll {

  override protected def afterAll(): Unit = {
    if (adminClient != null) adminClient.close()
    if (producer != null) producer.close()
    if (consumer != null) consumer.close()
    kafka.close()
    super.afterAll()
  }

  private def deleteResources(): Future[Assertion] = {
    var batchRequest: FhirBatchTransactionRequestBuilder = onFhirClient.batch()
    batchRequest = batchRequest.entry(_.delete("Patient", FhirMappingUtility.getHashedId("Patient", "p1")))
    onFhirClient.search("Observation").where("subject", "Patient/" + FhirMappingUtility.getHashedId("Patient", "p1"))
      .executeAndReturnBundle() flatMap { obsBundle =>
      obsBundle.searchResults.foreach(obs =>
        batchRequest = batchRequest.entry(_.delete("Observation", (obs \ "id").extract[String]))
      )
      batchRequest.returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
        res.httpStatus shouldBe StatusCodes.OK
      }
    }
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
    Map("source" -> KafkaSourceSettings("kafka-source", "https://aiccelerate.eu/data-integration-suite/kafka-data", s"PLAINTEXT://localhost:$kafkaPort"))


  val fhirSinkSettings: FhirRepositorySinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost:8081/fhir", errorHandling = Some(fhirWriteErrorHandling))

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> KafkaSource(topicName = "patients", groupId = "tofhir", startingOffsets = "earliest"))
  )
  val otherObservationMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/other-observation-mapping",
    sourceContext = Map("source" -> KafkaSource(topicName = "observations", groupId = "tofhir", startingOffsets = "earliest"))
  )

  implicit val actorSystem: ActorSystem = ActorSystem("StreamingTest")
  val onFhirClient: OnFhirNetworkClient = OnFhirNetworkClient.apply(fhirSinkSettings.fhirRepoUrl)
  val fhirServerIsAvailable: Boolean =
    Try(Await.result(onFhirClient.search("Patient").execute(), FiniteDuration(5, TimeUnit.SECONDS)).httpStatus == StatusCodes.OK)
      .getOrElse(false)

  val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, ErrorHandlingType.HALT)

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
    producer.send(new ProducerRecord[String, String](topicName, "1", "{\"pid\":\"p1\",\"time\":\"2007-10-12T10:00:00+01:00\",\"encounterId\":\"e1\",\"code\":\"9110-8\",\"value\":\"450\"}")).get
    Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () => {
      def foo(): Boolean = {
        val records = consumer.poll(Duration.ofMillis(100))
        if (records.isEmpty) return false
        records.iterator.next.topic shouldBe "observations"
        records.iterator.next.key shouldBe "1"
        records.iterator.next.value shouldBe "{\"pid\":\"p1\",\"time\":\"2007-10-12T10:00:00+01:00\",\"encounterId\":\"e1\",\"code\":\"9110-8\",\"value\":\"450\"}"
        true
      }
      foo()
    })
    consumer.unsubscribe()
  }

  it should "consume patients and observations data and map and write to the fhir repository" in {
    assume(fhirServerIsAvailable)
    val streamingQuery: StreamingQuery = fhirMappingJobManager.startMappingJobStream(tasks = Seq(patientMappingTask, otherObservationMappingTask), sourceSettings = streamingSourceSettings, sinkSettings = fhirSinkSettings)
    streamingQuery.awaitTermination(20000L) //wait for 30 seconds to consume and write to the fhir repo and terminate
  }

  it should "test whether fhir resources are written successfully" in {
    assume(fhirServerIsAvailable)
    val searchTest = onFhirClient.read("Patient", FhirMappingUtility.getHashedId("Patient", "p1")).executeAndReturnResource() flatMap { p1Resource =>
      FHIRUtil.extractIdFromResource(p1Resource) shouldBe FhirMappingUtility.getHashedId("Patient", "p1")
      FHIRUtil.extractValue[String](p1Resource, "gender") shouldBe "male"
      FHIRUtil.extractValue[String](p1Resource, "birthDate") shouldBe "1995-11-10"

      onFhirClient.search("Observation").where("code", "9110-8").executeAndReturnBundle() map { observationBundle =>
        (observationBundle.searchResults.head \ "subject" \ "reference").extract[String] shouldBe
          FhirMappingUtility.getHashedReference("Patient", "p1")
        deleteResources()
      }
    }
    Await.result(searchTest, duration.Duration(20, duration.SECONDS))
  }

}

