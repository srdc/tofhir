package io.ignifyr.engine.spi

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.json4s.Serializer

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Central, lazily-built registry of everything the installed [[IgnifyrExtension]] modules
 * contribute. Discovery is via [[java.util.ServiceLoader]] over the classpath (the shaded jars
 * already merge `META-INF/services` through the shade `ServicesResourceTransformer`).
 *
 * Lookup maps are keyed by model class (or command token) and built once, on first access. A key
 * claimed by more than one extension is a configuration error and fails fast, naming both owners.
 * Absence is not an error here — it surfaces as a `Missing*Exception` at the call site, so a job
 * referencing an uninstalled feature parses fine and fails only when it tries to use it.
 */
object ExtensionRegistry {

  private val logger: Logger = Logger(this.getClass)

  /** All discovered extensions, ordered by id, each initialized with its scoped config subtree. */
  lazy val extensions: Seq[IgnifyrExtension] = {
    val rootConfig: Config =
      Try(io.ignifyr.engine.Execution.actorSystem.settings.config).getOrElse(ConfigFactory.load())
    val loaded =
      ServiceLoader
        .load(classOf[IgnifyrExtension], classOf[IgnifyrExtension].getClassLoader)
        .iterator()
        .asScala
        .toSeq
        .sortBy(_.id)
    loaded.foreach { ext =>
      val path = s"ignifyr.extensions.${ext.id}"
      val scoped = if (rootConfig.hasPath(path)) rootConfig.getConfig(path) else ConfigFactory.empty()
      ext.initialize(scoped)
    }
    logger.info(s"Loaded ${loaded.size} Ignifyr extension(s): ${loaded.map(_.id).mkString(", ")}")
    loaded
  }

  lazy val sourceConnectors: Map[Class[_], SourceConnector] =
    indexUnique("source connector")(
      extensions.flatMap(e => e.sourceConnectors.map(c => (e.id, c.bindingClass: Class[_], c)))
    )

  lazy val sinkProviders: Map[Class[_], SinkProvider] =
    indexUnique("sink provider")(
      extensions.flatMap(e => e.sinkProviders.map(p => (e.id, p.settingsClass: Class[_], p)))
    )

  lazy val terminologyServiceProviders: Map[Class[_], TerminologyServiceProvider] =
    indexUnique("terminology service provider")(
      extensions.flatMap(e => e.terminologyServiceProviders.map(p => (e.id, p.settingsClass: Class[_], p)))
    )

  lazy val identityServiceProviders: Map[Class[_], IdentityServiceProvider] =
    indexUnique("identity service provider")(
      extensions.flatMap(e => e.identityServiceProviders.map(p => (e.id, p.settingsClass: Class[_], p)))
    )

  lazy val cliCommands: Map[String, CliCommandProvider] =
    indexUnique("CLI command")(
      extensions.flatMap(e => e.cliCommands.flatMap(cmd => (cmd.name +: cmd.aliases).map(token => (e.id, token, cmd))))
    )

  /**
   * The job-model classes the installed modules define (see [[IgnifyrExtension.jobModelClasses]]), keyed by
   * the simple class name a job JSON names them with (`jsonClass`). A name claimed by two modules fails fast;
   * a clash with one of the engine's own classes is detected when `FhirMappingJobFormatter.formats` is built.
   */
  lazy val jobModelClasses: Map[String, Class[_]] =
    indexUnique("job model class")(
      extensions.flatMap(e => e.jobModelClasses.map(c => (e.id, c.getSimpleName, c: Class[_])))
    )

  /** The custom json4s serializers of the installed modules' job-model classes, in extension order. */
  lazy val jobSerializers: Seq[Serializer[_]] =
    extensions.flatMap(_.jobSerializers)

  /** All source-failure descriptors, in extension order. Queried on a connector-specific failure. */
  lazy val sourceFailureDescriptors: Seq[SourceFailureDescriptor] =
    extensions.flatMap(_.sourceFailureDescriptors)

  lazy val schemaInferrers: Map[Class[_], SourceSchemaInferrer] =
    indexUnique("source schema inferrer")(
      extensions.flatMap(e => e.schemaInferrers.map(p => (e.id, p.settingsClass: Class[_], p)))
    )

  /** All installed lookup observers, in extension order. Several may coexist. */
  lazy val lookupObservers: Seq[MappingLookupObserver] =
    extensions.flatMap(_.lookupObservers)

  /**
   * A recorder for the lookups of the given task execution, fanning out to every installed observer, or
   * [[LookupRecorder.NoOp]] when none is installed. Driver side.
   */
  def lookupRecorderFor(spark: org.apache.spark.sql.SparkSession, scope: MappingLookupScope): LookupRecorder =
    lookupObservers match {
      case Seq() => LookupRecorder.NoOp
      case Seq(single) => single.recorderFor(spark, scope)
      case many => LookupRecorder.Composite(many.map(_.recorderFor(spark, scope)))
    }

  /** Notifies every installed lookup observer that a chunk / micro-batch of the task execution is done. Driver side. */
  def notifyLookupChunkCompleted(
      mappingJobExecution: io.ignifyr.engine.model.FhirMappingJobExecution,
      mappingTaskName: String
  ): Unit =
    lookupObservers.foreach { observer =>
      try observer.onChunkCompleted(mappingJobExecution, mappingTaskName)
      catch {
        // Monitoring must never fail the mapping itself
        case t: Throwable => logger.warn(s"Lookup observer ${observer.getClass.getName} failed on chunk completion", t)
      }
    }

  /** The single installed streaming execution provider, if any. More than one is a config error. */
  lazy val streaming: Option[StreamingExecutionProvider] = singleCapability("streaming execution provider")(
    extensions.flatMap(e => e.streamingProvider.map(e.id -> _))
  )

  /** The single installed resource validator provider, if any. More than one is a config error. */
  lazy val resourceValidator: Option[ResourceValidatorProvider] = singleCapability("resource validator provider")(
    extensions.flatMap(e => e.resourceValidatorProvider.map(e.id -> _))
  )

  /** The single installed scheduling execution provider, if any. More than one is a config error. */
  lazy val scheduler: Option[SchedulerProvider] = singleCapability("scheduling execution provider")(
    extensions.flatMap(e => e.schedulerProvider.map(e.id -> _))
  )

  /**
   * Spark-configuration entries contributed by the installed extensions, merged into the shared
   * SparkSession config by [[io.ignifyr.engine.config.IgnifyrConfig]]. `spark.sql.extensions` is a
   * comma-separated, additive list, so multiple contributors are concatenated (deduplicated); any
   * other key claimed by more than one extension is a configuration error and fails fast.
   *
   * Note this is the *cross-module* merge only, and it is stricter than the *cross-layer* one:
   * `IgnifyrConfig.additiveSparkConfKeys` joins a wider set of registration-list keys across the
   * engine/contribution/user layers, but here only `spark.sql.extensions` is additive. So two modules
   * both contributing e.g. `spark.plugins` still fail fast, even though that key would be joined if it
   * came from different layers. Widen the check here if a second additive key ever has two owners.
   */
  lazy val sparkConfContributions: Map[String, String] =
    mergeSparkConf(extensions.flatMap(e => e.sparkConfContributions.map { case (k, v) => (e.id, k, v) }))

  /**
   * Merge `(ownerExtensionId, key, value)` Spark-conf triples, concatenating the additive
   * `spark.sql.extensions` and failing on any other key claimed twice. Visible to `io.ignifyr` for the
   * same reason as [[indexUnique]].
   */
  private[ignifyr] def mergeSparkConf(entries: Seq[(String, String, String)]): Map[String, String] = {
    entries.groupBy(_._2).map { case (key, group) =>
      if (key == "spark.sql.extensions") {
        key -> group.map(_._3).distinct.mkString(",")
      } else if (group.size > 1) {
        val owners = group.map(_._1).mkString(", ")
        throw new IllegalStateException(
          s"Conflicting Spark configuration '$key' contributed by extensions: $owners. " +
            "Each Spark-conf key (other than the additive spark.sql.extensions) may be set by at most one module."
        )
      } else {
        key -> group.head._3
      }
    }
  }

  /**
   * Selects at most one capability provider, failing fast (naming owners) if more than one is installed.
   * Visible to `io.ignifyr` so the fail-fast contract can be asserted without a second classloader.
   */
  private[ignifyr] def singleCapability[V](what: String)(entries: Seq[(String, V)]): Option[V] = {
    if (entries.size > 1) {
      throw new IllegalStateException(
        s"Multiple $what modules installed: ${entries.map(_._1).mkString(", ")}. Install exactly one."
      )
    }
    entries.headOption.map(_._2)
  }

  /**
   * Force every registry that can reject its input to materialize — so a duplicate registration or a
   * second copy of a single-capability module surfaces at engine startup rather than in the middle of
   * a job — and log what was loaded.
   *
   * `streaming`, `scheduler` and `resourceValidator` are included deliberately: all throw from [[singleCapability]] when
   * two providers are installed, and neither is read anywhere else during startup. (`streaming` used
   * to be covered only incidentally, by `IgnifyrEngine` consulting it to decide whether to start the
   * archive timer; that is a coincidence, not a guarantee.) `sparkConfContributions` needs no entry —
   * building the shared SparkSession reads it, which happens before this call. The job (de)serialization
   * formats are built too, so a module's job-model class clashing with one of the engine's surfaces here.
   */
  def init(): Unit = {
    sourceConnectors
    sinkProviders
    terminologyServiceProviders
    identityServiceProviders
    cliCommands
    schemaInferrers
    jobModelClasses
    io.ignifyr.engine.util.FhirMappingJobFormatter.formats
    streaming
    scheduler
    resourceValidator
    ()
  }

  /**
   * Index `(ownerExtensionId, key, value)` triples into a `key -> value` map, failing with both
   * owner ids if any key is claimed twice. Visible to `io.ignifyr` for the same reason as
   * [[singleCapability]].
   */
  private[ignifyr] def indexUnique[K, V](what: String)(entries: Seq[(String, K, V)]): Map[K, V] =
    entries.groupBy(_._2).map { case (key, group) =>
      if (group.size > 1) {
        val owners = group.map(_._1).mkString(", ")
        throw new IllegalStateException(
          s"Duplicate $what registration for key '$key' from extensions: $owners. " +
            "Each key may be provided by exactly one installed module."
        )
      }
      key -> group.head._3
    }
}
