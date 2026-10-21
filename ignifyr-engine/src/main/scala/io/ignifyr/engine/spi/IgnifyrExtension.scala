package io.ignifyr.engine.spi

import com.typesafe.config.Config
import org.json4s.Serializer

/**
 * Aggregate service-provider interface for an Ignifyr edition module.
 *
 * A single [[IgnifyrExtension]] is discovered per module through [[java.util.ServiceLoader]]
 * (one `META-INF/services/io.ignifyr.engine.spi.IgnifyrExtension` entry per jar). Everything a
 * module contributes to the engine — source connectors, sinks, services, CLI commands, … — hangs
 * off this one descriptor, so moving a feature between the community and enterprise editions is a
 * plain folder move plus a reactor edit, with no engine code change.
 *
 * All contribution accessors default to empty; a module overrides only what it provides. Further
 * accessors (function libraries, streaming/scheduling capabilities, file-format handlers) are
 * introduced by later migration phases as they are needed — adding a defaulted method here stays
 * binary/source compatible with existing extensions.
 */
trait IgnifyrExtension {

  /**
   * Stable, unique identifier for this extension (e.g. "core", "connector-kafka").
   * Also selects the `ignifyr.extensions.<id>` HOCON subtree passed to [[initialize]].
   */
  def id: String

  /**
   * Called once during registry load with this extension's scoped configuration
   * (`ignifyr.extensions.<id>`, or an empty config if that block is absent).
   *
   * MUST NOT touch `IgnifyrConfig.sparkSession`: registry load is transitively triggered while the
   * shared SparkSession is being built (the session's config consults
   * [[ExtensionRegistry.sparkConfContributions]], which forces the extension list and hence every
   * `initialize`), so reading the session here would re-enter its lazy initialization and self-recurse.
   * Do Spark-dependent setup lazily, on first use, not in `initialize`.
   *
   * For the same reason it MUST NOT (de)serialize a job with `FhirMappingJobFormatter.formats`: building
   * those formats loads the registry to collect [[jobModelClasses]].
   */
  def initialize(config: Config): Unit = ()

  /** Data-source readers this module contributes, keyed at registration by their binding class. */
  def sourceConnectors: Seq[SourceConnector] = Nil

  /** Sink writers this module contributes, keyed at registration by their sink-settings class. */
  def sinkProviders: Seq[SinkProvider] = Nil

  /** Terminology-service providers, keyed at registration by their settings class. */
  def terminologyServiceProviders: Seq[TerminologyServiceProvider] = Nil

  /** Identity-service providers, keyed at registration by their settings class. */
  def identityServiceProviders: Seq[IdentityServiceProvider] = Nil

  /** CLI commands this module contributes, keyed at registration by name and aliases. */
  def cliCommands: Seq[CliCommandProvider] = Nil

  /** Source-failure descriptors this module contributes (e.g. a connector's client-specific errors). */
  def sourceFailureDescriptors: Seq[SourceFailureDescriptor] = Nil

  /** Source schema inferrers this module contributes, keyed at registration by their settings class. */
  def schemaInferrers: Seq[SourceSchemaInferrer] = Nil

  /** Streaming execution capability, if this module provides one. At most one may be installed. */
  def streamingProvider: Option[StreamingExecutionProvider] = None

  /** Scheduled (cron) execution capability, if this module provides one. At most one may be installed. */
  def schedulerProvider: Option[SchedulerProvider] = None

  /**
   * Observers of the terminology / concept-map / unit-conversion lookups mappings perform (e.g. to report
   * mapping coverage). Any number may be installed; each receives every observed lookup.
   */
  def lookupObservers: Seq[MappingLookupObserver] = Nil

  /**
   * Extra Spark configuration entries this module contributes to the shared SparkSession — e.g. an
   * enterprise format that needs a Spark session extension or catalog (`spark.sql.extensions`,
   * `spark.sql.catalog.spark_catalog`). Merged into the session config when it is first built.
   *
   * Precedence, lowest first: engine defaults, these contributions, the user's `spark { }` block. For
   * a single-valued key the user's block wins. For a key Spark parses as a comma-separated
   * registration list (`spark.sql.extensions` and the rest of `IgnifyrConfig.additiveSparkConfKeys`)
   * no layer wins — the values are joined and deduplicated, so a user setting
   * `spark.sql.extensions` for an unrelated reason cannot silently drop the extension this module
   * registers.
   *
   * Keeping this a pure accessor that never touches the SparkSession avoids an initialization cycle.
   */
  def sparkConfContributions: Map[String, String] = Map.empty

  /**
   * Job-model classes this module defines — the source bindings, source settings, sink settings, … its
   * connectors and sinks read from a mapping job — so that a job JSON naming them by `jsonClass` parses.
   * They join the engine's own classes in `FhirMappingJobFormatter.formats` (a json4s `ShortTypeHints`
   * table, i.e. keyed by simple class name). A simple name claimed twice, by two modules or by a module
   * and the engine, is a configuration error and fails fast. A job naming a class no installed module
   * provides fails to parse with a message saying so.
   *
   * Keep the class next to the connector/sink that reads it, in the module itself: the engine does not need
   * to know it, which is what lets a module add a source or sink type without an engine change.
   */
  def jobModelClasses: Seq[Class[_]] = Nil

  /**
   * Custom json4s serializers for this module's [[jobModelClasses]], if the default case-class
   * (de)serialization does not fit (e.g. a defaulted field that must still be written). Added to
   * `FhirMappingJobFormatter.formats`.
   */
  def jobSerializers: Seq[Serializer[_]] = Nil

  /**
   * Extra, human-readable capability lines for the `list-plugins` command — for contributions the
   * engine's typed registries cannot introspect on their own, such as a module's own sub-registry
   * (e.g. the file connector's discovered source/sink formats). Purely descriptive; empty by default.
   */
  def extraCapabilities: Seq[String] = Nil
}
