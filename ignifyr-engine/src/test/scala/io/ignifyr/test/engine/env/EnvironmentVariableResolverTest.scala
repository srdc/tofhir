package io.ignifyr.test.engine.env

import io.ignifyr.engine.env.EnvironmentVariableResolver
import io.ignifyr.engine.model._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A module's binding, resolving the placeholders of its own field. */
private case class TopicSource(topic: String) extends MappingSourceBinding {
  override def withPreprocessSql(preprocessSql: Option[String]): MappingSourceBinding = this
  override def withResolvedEnvironmentVariables(resolve: String => String): MappingSourceBinding =
    copy(topic = resolve(topic))
}

/**
 * Covers `${ENV_VAR}` substitution in mapping-job definitions. Only the names listed in the
 * `EnvironmentVariable` enumeration are substitutable; anything else is rejected rather than silently
 * left in place, which is what makes a typo in a job file a startup error instead of a bad FHIR write.
 *
 * `resolveFileContent` is exercised against an explicit environment (`sys.env` cannot be set from
 * inside the JVM); the object-level paths need no environment because they are the failure paths.
 */
class EnvironmentVariableResolverTest extends AnyFlatSpec with Matchers {

  private val env = Map("FHIR_REPO_URL" -> "http://onfhir:8080/fhir", "DATA_FOLDER_PATH" -> "/data")

  private def jobWith(
      sourceSettings: Map[String, MappingJobSourceSettings],
      sinkSettings: SinkSettings = FhirRepositorySinkSettings(fhirRepoUrl = "http://localhost/fhir"),
      mappings: Seq[FhirMappingTask] = Seq.empty
  ): FhirMappingJob =
    FhirMappingJob(sourceSettings = sourceSettings, sinkSettings = sinkSettings, mappings = mappings)

  "resolveFileContent" should "replace a placeholder with the value from the environment" in {
    EnvironmentVariableResolver.resolveFileContent("""{"fhirRepoUrl": "${FHIR_REPO_URL}"}""", env) shouldBe
      """{"fhirRepoUrl": "http://onfhir:8080/fhir"}"""
  }

  it should "replace every occurrence of every known placeholder" in {
    EnvironmentVariableResolver.resolveFileContent(
      "${DATA_FOLDER_PATH}:${FHIR_REPO_URL}:${DATA_FOLDER_PATH}",
      env
    ) shouldBe
      "/data:http://onfhir:8080/fhir:/data"
  }

  it should "leave a known placeholder untouched when the variable is not set" in {
    EnvironmentVariableResolver.resolveFileContent("${SOURCE_URL}", env) shouldBe "${SOURCE_URL}"
  }

  it should "leave content with no placeholder unchanged" in {
    val content = """{"name": "no placeholders here"}"""
    EnvironmentVariableResolver.resolveFileContent(content, env) shouldBe content
  }

  "resolveFhirMappingJob" should "leave settings without a placeholder unchanged" in {
    val job = jobWith(
      Map("main" -> FileSystemSourceSettings(name = "src", sourceUri = "urn:test", dataFolderPath = "/plain/path"))
    )
    val resolved = EnvironmentVariableResolver.resolveFhirMappingJob(job)
    resolved.sourceSettings("main").asInstanceOf[FileSystemSourceSettings].dataFolderPath shouldBe "/plain/path"
    resolved.sinkSettings.asInstanceOf[FhirRepositorySinkSettings].fhirRepoUrl shouldBe "http://localhost/fhir"
  }

  it should "reject a placeholder whose name is not in the EnvironmentVariable enumeration" in {
    val job = jobWith(
      Map(
        "main" -> FileSystemSourceSettings(name = "src", sourceUri = "urn:test", dataFolderPath = "${NOT_A_KNOWN_VAR}")
      )
    )
    val thrown = the[RuntimeException] thrownBy EnvironmentVariableResolver.resolveFhirMappingJob(job)
    thrown.getMessage should include("NOT_A_KNOWN_VAR")
    thrown.getMessage should include("not recognized")
  }

  it should "reject a known placeholder that is not set in the environment" in {
    // SOURCE_URL is a legal name, so this fails on the value being absent rather than on the name.
    val job = jobWith(
      Map("main" -> FileSystemSourceSettings(name = "src", sourceUri = "urn:test", dataFolderPath = "${SOURCE_URL}")),
      sinkSettings = FileSystemSinkSettings(path = "./out", contentType = SinkContentTypes.NDJSON)
    )
    assume(sys.env.get("SOURCE_URL").isEmpty, "SOURCE_URL must be unset for this failure path")
    val thrown = the[RuntimeException] thrownBy EnvironmentVariableResolver.resolveFhirMappingJob(job)
    thrown.getMessage should include("SOURCE_URL")
    thrown.getMessage should include("is not set")
  }

  it should "let a source binding type resolve the placeholders of its own fields" in {
    val job = jobWith(
      Map("main" -> FileSystemSourceSettings(name = "src", sourceUri = "urn:test", dataFolderPath = "/data")),
      mappings = Seq(
        FhirMappingTask(
          name = "t",
          mappingRef = "http://test/mappings/t",
          sourceBinding = Map("source" -> TopicSource("${NOT_A_KNOWN_VAR}"))
        )
      )
    )
    val thrown = the[RuntimeException] thrownBy EnvironmentVariableResolver.resolveFhirMappingJob(job)
    thrown.getMessage should include("NOT_A_KNOWN_VAR")
  }

  it should "not touch a sink type that carries no resolvable field" in {
    val fileSink = FileSystemSinkSettings(path = "./out", contentType = SinkContentTypes.NDJSON)
    val job = jobWith(
      Map("main" -> FileSystemSourceSettings(name = "src", sourceUri = "urn:test", dataFolderPath = "/data")),
      sinkSettings = fileSink
    )
    EnvironmentVariableResolver.resolveFhirMappingJob(job).sinkSettings shouldBe fileSink
  }

  "getEnvironmentVariables" should "only report names declared in the enumeration" in {
    val known = Set("FHIR_REPO_URL", "DATA_FOLDER_PATH", "SOURCE_URL", "REDCAP_PROJECT_ID")
    EnvironmentVariableResolver.getEnvironmentVariables.keySet.diff(known) shouldBe empty
  }
}
