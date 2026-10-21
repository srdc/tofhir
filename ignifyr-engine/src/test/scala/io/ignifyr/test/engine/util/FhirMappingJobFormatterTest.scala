package io.ignifyr.test.engine.util

import io.onfhir.client.model.BasicAuthenticationSettings
import io.ignifyr.engine.model._
import io.ignifyr.engine.util.FhirMappingJobFormatter
import org.json4s.JsonAST.{JBool, JObject, JString}
import org.json4s.jackson.{JsonMethods, Serialization}
import org.json4s.{CustomSerializer, Formats, MappingException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/** Job-model classes of a module that is not the engine: a source type the engine does not know. */
private case class ModuleSource(queue: String) extends MappingSourceBinding {
  override def withPreprocessSql(preprocessSql: Option[String]): MappingSourceBinding = this
}

private case class ModuleSourceSettings(name: String, sourceUri: String, enabled: Boolean = true)
    extends MappingJobSourceSettings

/** A module serializer writing the defaulted `enabled`, as the Kafka connector does for `asStream`. */
private object ModuleSourceSettingsSerializer
    extends CustomSerializer[ModuleSourceSettings](_ =>
      (
        { case json: JObject =>
          implicit val formats: Formats = org.json4s.DefaultFormats
          ModuleSourceSettings(
            (json \ "name").extract[String],
            (json \ "sourceUri").extract[String],
            (json \ "enabled").extractOrElse(false)
          )
        },
        { case settings: ModuleSourceSettings =>
          JObject(
            "name" -> JString(settings.name),
            "sourceUri" -> JString(settings.sourceUri),
            "enabled" -> JBool(settings.enabled),
            "jsonClass" -> JString("ModuleSourceSettings")
          )
        }
      )
    )

/** A module class with the simple name of an engine one. */
private object clashing {
  case class SqlSource(table: String)
}

/**
 * Covers the mapping-job (de)serializer: the `ShortTypeHints` table that lets one job file carry any
 * source/sink/scheduling type — the engine's own and those the installed modules contribute — and the
 * duplicate-mappingTask-name rejection. The server rejects duplicate names on its own create path; this
 * is the file-read path the CLI and the scheduler use.
 */
class FhirMappingJobFormatterTest extends AnyFlatSpec with Matchers {

  private def task(name: String, binding: MappingSourceBinding = FileSystemSource("p.csv", SourceContentTypes.CSV)) =
    FhirMappingTask(name = name, mappingRef = s"http://test/mappings/$name", sourceBinding = Map("source" -> binding))

  /** Round-trips a job through a temp file, the way the CLI reads a job handed to `run --job`. */
  private def roundTrip(job: FhirMappingJob): FhirMappingJob = {
    val file = Files.createTempFile("ignifyr-job", ".json")
    FhirMappingJobFormatter.saveMappingJobToFile(job, file.toString)
    FhirMappingJobFormatter.readMappingJobFromFile(file.toString)
  }

  "findDuplicateMappingTaskNames" should "return nothing when every name is unique" in {
    FhirMappingJobFormatter.findDuplicateMappingTaskNames(Seq(task("a"), task("b"))) shouldBe empty
  }

  it should "return each repeated name once" in {
    val duplicates =
      FhirMappingJobFormatter.findDuplicateMappingTaskNames(Seq(task("a"), task("a"), task("a"), task("b"), task("b")))
    duplicates should contain theSameElementsAs Seq("a", "b")
  }

  "readMappingJobFromFile" should "restore every engine source and sink type" in {
    val job = FhirMappingJob(
      name = Some("multi-source-job"),
      sourceSettings = Map(
        "file" -> FileSystemSourceSettings(name = "f", sourceUri = "urn:f", dataFolderPath = "./data"),
        "sql" -> SqlSourceSettings(
          name = "s",
          sourceUri = "urn:s",
          databaseUrl = "jdbc:h2:mem:t",
          username = "u",
          password = "p"
        )
      ),
      sinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://onfhir/fhir"),
      mappings = Seq(
        task("from-file"),
        task("from-sql", SqlSource(tableName = Some("patients")))
      )
    )

    val restored = roundTrip(job)
    restored.sourceSettings("file") shouldBe a[FileSystemSourceSettings]
    restored.sourceSettings("sql") shouldBe a[SqlSourceSettings]
    restored.sinkSettings shouldBe a[FhirRepositorySinkSettings]
    restored.mappings.map(_.sourceBinding("source").getClass) shouldBe
      job.mappings.map(_.sourceBinding("source").getClass)
  }

  it should "restore the sink security settings" in {
    val job = FhirMappingJob(
      sourceSettings = Map("file" -> FileSystemSourceSettings(name = "f", sourceUri = "urn:f", dataFolderPath = "./d")),
      sinkSettings = FhirRepositorySinkSettings(
        fhirRepoUrl = "http://secured/fhir",
        securitySettings = Some(BasicAuthenticationSettings("user", "secret"))
      ),
      mappings = Seq(task("a"))
    )

    val restored = roundTrip(job).sinkSettings.asInstanceOf[FhirRepositorySinkSettings]
    restored.securitySettings shouldBe Some(BasicAuthenticationSettings("user", "secret"))
  }

  it should "restore a file sink with its content type and options" in {
    val sink = FileSystemSinkSettings(
      path = "./out",
      contentType = SinkContentTypes.CSV,
      options = Map("header" -> "true")
    )
    val job = FhirMappingJob(
      sourceSettings = Map("file" -> FileSystemSourceSettings(name = "f", sourceUri = "urn:f", dataFolderPath = "./d")),
      sinkSettings = sink,
      mappings = Seq(task("a"))
    )
    roundTrip(job).sinkSettings shouldBe sink
  }

  "formats" should "read and write the job-model classes and serializers a module contributes" in {
    implicit val formats: Formats =
      FhirMappingJobFormatter.buildFormats(
        Seq(classOf[ModuleSource], classOf[ModuleSourceSettings]),
        Seq(ModuleSourceSettingsSerializer)
      )
    val job = FhirMappingJob(
      sourceSettings = Map("queue" -> ModuleSourceSettings(name = "q", sourceUri = "urn:q")),
      sinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://onfhir/fhir"),
      mappings = Seq(task("from-queue", ModuleSource("patients")))
    )
    val json = Serialization.write(job)
    // The module serializer writes the defaulted field (its deserializer would read it as false otherwise)
    json should include(""""enabled":true""")

    val restored = JsonMethods.parse(json).extract[FhirMappingJob]
    restored.sourceSettings("queue") shouldBe ModuleSourceSettings(name = "q", sourceUri = "urn:q")
    restored.mappings.head.sourceBinding("source") shouldBe ModuleSource("patients")
  }

  it should "fail on a jsonClass no installed module provides, saying so" in {
    // The engine's test classpath installs no module defining job-model classes
    val json =
      """{"sourceSettings": {"q": {"jsonClass": "ModuleSourceSettings", "name": "q", "sourceUri": "urn:q"}},
        | "sinkSettings": {"jsonClass": "FhirRepositorySinkSettings", "fhirRepoUrl": "http://onfhir/fhir"},
        | "mappings": []}""".stripMargin
    val file = Files.createTempFile("ignifyr-job", ".json")
    Files.writeString(file, json)

    val thrown = the[MappingException] thrownBy FhirMappingJobFormatter.readMappingJobFromFile(file.toString)
    thrown.getMessage should include("Unknown jsonClass 'ModuleSourceSettings'")
    thrown.getMessage should include("no installed module provides it")
  }

  it should "reject a module job-model class reusing the name of an engine one" in {
    val thrown = the[IllegalStateException] thrownBy FhirMappingJobFormatter.buildFormats(
      Seq(classOf[clashing.SqlSource]),
      Nil
    )
    thrown.getMessage should include(classOf[clashing.SqlSource].getName)
    thrown.getMessage should include("reuse the name of an engine job model class")
  }

  "readMappingJobFromFile" should "reject a job whose mappingTasks share a name" in {
    val job = FhirMappingJob(
      sourceSettings = Map("file" -> FileSystemSourceSettings(name = "f", sourceUri = "urn:f", dataFolderPath = "./d")),
      sinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://onfhir/fhir"),
      mappings = Seq(task("duplicated"), task("duplicated"))
    )
    val file = Files.createTempFile("ignifyr-job", ".json")
    FhirMappingJobFormatter.saveMappingJobToFile(job, file.toString)

    val thrown = the[MappingException] thrownBy FhirMappingJobFormatter.readMappingJobFromFile(file.toString)
    thrown.getMessage should include("duplicated")
    thrown.getMessage should include("unique name")
  }
}
