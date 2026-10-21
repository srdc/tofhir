package io.ignifyr.engine.spi

import io.ignifyr.engine.model.{FhirRepositorySinkSettings, FileSystemSinkSettings, FileSystemSource, SqlSource}

/**
 * Static "which module provides this?" hints used only to build actionable error messages when a
 * connector/sink is missing. This is not dispatch logic — it maps a model class to a human label
 * and the Maven coordinates of the module that supplies its runtime, so the error can say exactly
 * what to install. Unknown (e.g. third-party) classes fall back to their simple name.
 *
 * Keyed on the engine's own model classes, so these entries stay valid regardless of which edition
 * currently ships the corresponding reader/writer. A model class defined by a module (see
 * [[IgnifyrExtension.jobModelClasses]]) needs no entry: without the module the job does not parse, and the
 * parse error names the unknown `jsonClass`.
 */
object ExtensionHints {

  private val sourceModules: Map[Class[_], (String, String)] = Map(
    classOf[FileSystemSource] -> ("file", "io.ignifyr:ignifyr-connector-file"),
    classOf[SqlSource] -> ("sql", "io.ignifyr:ignifyr-connector-sql")
  )

  private val sinkModules: Map[Class[_], (String, String)] = Map(
    classOf[FhirRepositorySinkSettings] -> ("fhir-repository", "io.ignifyr:ignifyr-sink-fhir"),
    classOf[FileSystemSinkSettings] -> ("file", "io.ignifyr:ignifyr-sink-file")
  )

  private val cliCommandModules: Map[String, String] = Map(
    "extract-redcap-schemas" -> "com.pontegra.ignifyr:ignifyr-redcap"
  )

  def describeSource(bindingClass: Class[_]): String =
    describe("source reader", "source type", bindingClass, sourceModules)

  def describeSink(settingsClass: Class[_]): String =
    describe("sink writer", "sink type", settingsClass, sinkModules)

  def describeCliCommand(commandToken: String): String =
    cliCommandModules.get(commandToken) match {
      case Some(module) =>
        s"Unknown command '$commandToken'. It is provided by the '$module' module; install it to enable the command."
      case None =>
        s"Unknown command '$commandToken'."
    }

  private def describe(what: String, kind: String, cls: Class[_], modules: Map[Class[_], (String, String)]): String =
    modules.get(cls) match {
      case Some((label, module)) =>
        s"No $what registered for $kind '$label'. Install the '$module' module."
      case None =>
        s"No $what registered for '${cls.getSimpleName}'. The module providing it is not installed."
    }
}
