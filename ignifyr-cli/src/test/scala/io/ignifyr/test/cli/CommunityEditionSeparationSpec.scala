package io.ignifyr.test.cli

import io.ignifyr.connector.file.format.FileFormatRegistry
import io.ignifyr.engine.model._
import io.ignifyr.engine.spi.ExtensionRegistry
import io.ignifyr.engine.util.FhirMappingJobFormatter
import io.ignifyr.sink.file.format.FileSinkFormatRegistry
import org.json4s.MappingException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files

/**
 * Community-edition separation guard.
 *
 * This module (`ignifyr-cli`) has exactly the community distribution on its classpath — the engine
 * plus `connector-sql`, `connector-file`, `sink-fhir` and `sink-file`, and none of the enterprise
 * plugins. So the `ExtensionRegistry` and the file-format sub-registries observed here are exactly
 * what the shipped `ignifyr-engine-standalone.jar` discovers via ServiceLoader. The tests assert the
 * exact community set at each registration point, including the source/sink format sub-registries
 * (where `format-json`/`format-delta` plug in without a top-level `IgnifyrExtension`).
 */
class CommunityEditionSeparationSpec extends AnyFlatSpec with Matchers {

  behavior of "The community edition classpath (engine + connector-sql + connector-file + sink-fhir + sink-file)"

  it should "register exactly the community extension set" in {
    ExtensionRegistry.extensions.map(_.id).toSet shouldBe
      Set("core", "connector-sql", "connector-file", "sink-fhir", "sink-file")
  }

  it should "register exactly the community sinks" in {
    ExtensionRegistry.sinkProviders.keySet shouldBe
      Set[Class[_]](classOf[FhirRepositorySinkSettings], classOf[FileSystemSinkSettings])
  }

  it should "discover exactly the community file formats (source + sink sub-SPIs)" in {
    FileFormatRegistry.sourceFormats.keySet shouldBe Set("csv", "tsv", "parquet")
    FileSinkFormatRegistry.sinkFormats.keySet shouldBe Set("ndjson", "csv", "parquet")
  }

  it should "register no enterprise execution capability (no streaming, no scheduling)" in {
    ExtensionRegistry.streaming shouldBe None
    ExtensionRegistry.scheduler shouldBe None
  }

  it should "register only the community source connectors" in {
    ExtensionRegistry.sourceConnectors.keySet shouldBe Set[Class[_]](classOf[SqlSource], classOf[FileSystemSource])
  }

  it should "know no job-model class beyond the engine's own" in {
    ExtensionRegistry.jobModelClasses shouldBe empty
  }

  // The model of an enterprise source lives in its module: a job using one does not parse here, and says why
  it should "reject a job using an enterprise source type, naming the unknown jsonClass" in {
    val file = Files.createTempFile("ignifyr-job", ".json")
    Files.writeString(
      file,
      """{"sourceSettings": {"k": {"jsonClass": "KafkaSourceSettings", "name": "k", "sourceUri": "urn:k",
        |  "bootstrapServers": "localhost:9092"}},
        | "sinkSettings": {"jsonClass": "FhirRepositorySinkSettings", "fhirRepoUrl": "http://onfhir/fhir"},
        | "mappings": []}""".stripMargin
    )
    val thrown = the[MappingException] thrownBy FhirMappingJobFormatter.readMappingJobFromFile(file.toString)
    thrown.getMessage should include("Unknown jsonClass 'KafkaSourceSettings'")
  }

  it should "not expose enterprise CLI commands" in {
    ExtensionRegistry.cliCommands.keySet should not contain "extract-redcap-schemas"
  }
}
