package io.ignifyr.test.engine.spi

import io.ignifyr.engine.config.IgnifyrConfig
import io.ignifyr.engine.data.read.SourceHandler
import io.ignifyr.engine.model.{LocalFhirTerminologyServiceSettings, MappingJobSourceSettings, MappingSourceBinding}
import io.ignifyr.engine.spi.{ExtensionRegistry, MissingConnectorException}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A source binding with no registered connector, used to exercise the missing-connector path. */
private case class UnregisteredSource() extends MappingSourceBinding {
  override def withPreprocessSql(preprocessSql: Option[String]): MappingSourceBinding = this
}

private case class UnregisteredSourceSettings(name: String = "unregistered", sourceUri: String = "urn:test")
    extends MappingJobSourceSettings

/**
 * Verifies the ServiceLoader-based extension registry: the engine core registers what ships in the
 * engine itself (the local terminology service and the built-in CLI commands — every concrete
 * source and sink lives in its own module), and a source binding with no installed connector fails
 * with an actionable message (rather than the job failing to parse, or a bare NotImplementedError as
 * before). Extracted sources/sinks (SQL, file, FHIR repository, ...) are asserted in those modules'
 * registration specs, since the engine's own test classpath does not include them.
 */
class ExtensionRegistrySpec extends AnyFlatSpec with Matchers {

  behavior of "ExtensionRegistry"

  it should "discover the engine core's own registrations through ServiceLoader" in {
    val terminologies = ExtensionRegistry.terminologyServiceProviders.keySet
    terminologies should contain(classOf[LocalFhirTerminologyServiceSettings]: Class[_])
  }

  it should "raise an actionable MissingConnectorException when no connector is registered for a source binding" in {
    val ex = intercept[MissingConnectorException] {
      SourceHandler.readSource(
        alias = "test",
        spark = IgnifyrConfig.sparkSession,
        mappingSource = UnregisteredSource(),
        mappingJobSourceSettings = UnregisteredSourceSettings(),
        schema = None
      )
    }
    ex.getMessage should include("No source reader registered")
    ex.getMessage should include("UnregisteredSource")
  }

  it should "find no module job-model classes or serializers on the core-only classpath" in {
    // The engine core's job-model classes are its own (FhirMappingJobFormatter); only modules contribute any
    ExtensionRegistry.jobModelClasses shouldBe empty
    ExtensionRegistry.jobSerializers shouldBe empty
  }

  it should "materialize every rejecting registry without error on the core-only classpath" in {
    noException should be thrownBy ExtensionRegistry.init()
  }

  // The three fail-fast guards below are what `init()` exists to trigger at engine startup rather than
  // mid-job. They are asserted on the indexing helpers directly: the registries themselves are fed by
  // ServiceLoader, so a duplicate cannot be staged on this classpath without a second classloader.
  it should "index one registration per key" in {
    ExtensionRegistry.indexUnique[String, Int]("source reader")(Seq(("ext-a", "k1", 1), ("ext-b", "k2", 2))) shouldBe
      Map("k1" -> 1, "k2" -> 2)
  }

  it should "fail fast naming both owners when two extensions claim the same key" in {
    val ex = intercept[IllegalStateException] {
      ExtensionRegistry.indexUnique[String, Int]("source reader")(
        Seq(("ext-a", "same-key", 1), ("ext-b", "same-key", 2))
      )
    }
    ex.getMessage should include("Duplicate source reader registration")
    ex.getMessage should include("same-key")
    ex.getMessage should include("ext-a")
    ex.getMessage should include("ext-b")
  }

  it should "select the single installed capability provider, or none" in {
    ExtensionRegistry.singleCapability[String]("streaming")(Seq(("ext-a", "provider"))) shouldBe Some("provider")
    ExtensionRegistry.singleCapability[String]("streaming")(Seq.empty) shouldBe None
  }

  it should "fail fast naming both owners when two single-capability providers are installed" in {
    val ex = intercept[IllegalStateException] {
      ExtensionRegistry.singleCapability[String]("streaming")(Seq(("ext-a", "p1"), ("ext-b", "p2")))
    }
    ex.getMessage should include("Multiple streaming modules installed")
    ex.getMessage should include("ext-a")
    ex.getMessage should include("ext-b")
  }

  it should "concatenate the additive spark.sql.extensions contributed by several modules" in {
    ExtensionRegistry.mergeSparkConf(
      Seq(
        ("ext-a", "spark.sql.extensions", "ClassA"),
        ("ext-b", "spark.sql.extensions", "ClassB"),
        ("ext-c", "spark.sql.extensions", "ClassA") // duplicate value, contributed twice
      )
    ) shouldBe Map("spark.sql.extensions" -> "ClassA,ClassB")
  }

  it should "fail fast when two modules claim the same non-additive Spark-conf key" in {
    val ex = intercept[IllegalStateException] {
      ExtensionRegistry.mergeSparkConf(
        Seq(("ext-a", "spark.sql.catalog.spark_catalog", "A"), ("ext-b", "spark.sql.catalog.spark_catalog", "B"))
      )
    }
    ex.getMessage should include("Conflicting Spark configuration")
    ex.getMessage should include("spark.sql.catalog.spark_catalog")
    ex.getMessage should include("ext-a")
    ex.getMessage should include("ext-b")
  }
}
