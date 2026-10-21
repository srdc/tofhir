package io.ignifyr.engine.util

import io.onfhir.client.model.{
  BasicAuthenticationSettings,
  BearerTokenAuthorizationSettings,
  FixedTokenAuthenticationSettings
}
import io.ignifyr.engine.env.EnvironmentVariableResolver
import io.ignifyr.engine.model._
import io.ignifyr.engine.spi.ExtensionRegistry
import org.json4s.JsonAST.{JString, JValue}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, MappingException, Serializer, ShortTypeHints}

import java.lang.reflect.Modifier

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.io.Source

/**
 * (De)serialization of mapping jobs. A polymorphic member of a job (source binding, source/sink settings,
 * security settings, …) is written with a `jsonClass` field holding its simple class name, which selects the
 * class to read it back as.
 */
object FhirMappingJobFormatter {

  /** The job-model classes of the engine itself; the installed modules add theirs (see [[formats]]). */
  private[ignifyr] val engineJobModelClasses: List[Class[_]] = List(
    // Sink settings
    classOf[FhirRepositorySinkSettings],
    classOf[FileSystemSinkSettings],
    // Source types
    classOf[FileSystemSource],
    classOf[SqlSource],
    // Source settings
    classOf[FileSystemSourceSettings],
    classOf[SqlSourceSettings],
    // Authorization types
    classOf[BearerTokenAuthorizationSettings],
    classOf[BasicAuthenticationSettings],
    classOf[FixedTokenAuthenticationSettings],
    // Terminology services
    classOf[LocalFhirTerminologyServiceSettings],
    // Scheduling Settings
    classOf[SchedulingSettings],
    classOf[SQLSchedulingSettings]
  )

  /**
   * The formats every job (de)serialization uses: the engine's job-model classes plus those the installed
   * modules contribute ([[io.ignifyr.engine.spi.IgnifyrExtension.jobModelClasses]], `jobSerializers`). Built
   * on first use, which loads the extension registry.
   */
  implicit lazy val formats: Formats =
    buildFormats(ExtensionRegistry.jobModelClasses.values.toList, ExtensionRegistry.jobSerializers)

  /**
   * The job formats over the engine's job-model classes plus the given module ones. Fails if a module class
   * has the simple name of one of the engine's: a `jsonClass` must name exactly one class.
   */
  private[ignifyr] def buildFormats(moduleClasses: Seq[Class[_]], moduleSerializers: Seq[Serializer[_]]): Formats = {
    val engineNames = engineJobModelClasses.map(_.getSimpleName).toSet
    val clashing = moduleClasses.filter(c => engineNames.contains(c.getSimpleName))
    if (clashing.nonEmpty) {
      throw new IllegalStateException(
        s"Job model class(es) ${clashing.map(_.getName).mkString(", ")} contributed by an installed module " +
          "reuse the name of an engine job model class. A jsonClass must name exactly one class."
      )
    }
    val engineFormats: Formats = Serialization.formats(new JobModelTypeHints(engineJobModelClasses ++ moduleClasses))
    moduleSerializers.foldLeft(engineFormats)(_ + _)
  }

  /**
   * [[ShortTypeHints]] that, for a polymorphic member (declared as a trait or abstract class) whose `jsonClass`
   * names no known class, fail with a message saying that no installed module provides it. Plain json4s would
   * not treat an unknown `jsonClass` as a type hint and fail on the abstract type instead ("No constructor
   * for type ...").
   *
   * json4s probes the hints in two ways: [[typeHintFieldNameForHint]] (through `isTypeHintField` and
   * `typeHintFieldNameForClass`), which must answer, and [[classFor]], called only to extract a value whose type
   * hint was recognized, which is where an unknown one fails.
   */
  private final class JobModelTypeHints(classes: List[Class[_]]) extends ShortTypeHints(classes) {

    private def knownClassFor(hint: String, parent: Class[_]): Option[Class[_]] = super.classFor(hint, parent)

    override def typeHintFieldNameForHint(hint: String, parent: Class[_]): Option[String] =
      knownClassFor(hint, parent).map(_ => typeHintFieldName)

    override def isTypeHintField(field: (String, JValue), parent: Class[_]): Boolean = field match {
      case (name, JString(hint)) if name == typeHintFieldName =>
        knownClassFor(hint, parent).isDefined || parent.isInterface || Modifier.isAbstract(parent.getModifiers)
      case _ => false
    }

    override def classFor(hint: String, parent: Class[_]): Option[Class[_]] =
      knownClassFor(hint, parent).orElse(
        throw new MappingException(
          s"Unknown jsonClass '$hint' (a ${parent.getSimpleName}): no installed module provides it. " +
            "Install the module providing this type, or use another one."
        )
      )
  }

  /**
   *
   * @param fhirMappingJob
   * @param filePath
   */
  def saveMappingJobToFile(fhirMappingJob: FhirMappingJob, filePath: String): Unit = {
    Files.write(Paths.get(filePath), Serialization.writePretty(fhirMappingJob).getBytes(StandardCharsets.UTF_8))
  }

  /**
   *
   * @param filePath
   * @return
   */
  def readMappingJobFromFile(filePath: String): FhirMappingJob = {
    val source = Source.fromFile(filePath, StandardCharsets.UTF_8.name())
    val fileContent =
      try EnvironmentVariableResolver.resolveFileContent(source.mkString)
      finally source.close()
    val mappingJob = org.json4s.jackson.JsonMethods.parse(fileContent).extract[FhirMappingJob]
    // check there are no duplicate name on mappingTasks of the job
    val duplicateMappingTasks = FhirMappingJobFormatter.findDuplicateMappingTaskNames(mappingJob.mappings)
    if (duplicateMappingTasks.nonEmpty) {
      throw new MappingException(
        s"Duplicate 'name' fields detected in the MappingTasks of the MappingJob: ${duplicateMappingTasks.mkString(", ")}. Please ensure that each MappingTask has a unique name."
      )
    }
    mappingJob
  }

  /**
   * Check for duplicate names in the mappingTask array from the mappingJob definition.
   *
   * @param mappingTasks mappingTask array from the mappingJob definition.
   * @return A sequence of duplicate names, if any. Returns an empty sequence if all names are unique.
   */
  def findDuplicateMappingTaskNames(mappingTasks: Seq[FhirMappingTask]): Seq[String] = {
    mappingTasks.groupBy(_.name).view.mapValues(_.size).filter(_._2 > 1).keys.toSeq
  }

}
