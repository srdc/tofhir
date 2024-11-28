package io.tofhir.engine.util

import io.onfhir.client.model.{BasicAuthenticationSettings, BearerTokenAuthorizationSettings, FixedTokenAuthenticationSettings}
import io.tofhir.engine.env.EnvironmentVariableResolver
import io.tofhir.engine.model._
import org.json4s.jackson.Serialization
import org.json4s.{Formats, MappingException, ShortTypeHints}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.io.Source

/**
 *
 */
object FhirMappingJobFormatter {

  implicit lazy val formats: Formats =
    Serialization
      .formats(
        ShortTypeHints(List(
          //Sink settings
          classOf[FhirRepositorySinkSettings],
          classOf[FileSystemSinkSettings],
          //Source types
          classOf[FileSystemSource],
          classOf[KafkaSource],
          classOf[SqlSource],
          classOf[FhirServerSource],
          //Source settings
          classOf[FileSystemSourceSettings],
          classOf[SqlSourceSettings],
          classOf[KafkaSourceSettings],
          classOf[FhirServerSourceSettings],
          // Authorization types
          classOf[BearerTokenAuthorizationSettings],
          classOf[BasicAuthenticationSettings],
          classOf[FixedTokenAuthenticationSettings],
          //Terminology services
          classOf[LocalFhirTerminologyServiceSettings],
          //Scheduling Settings
          classOf[SchedulingSettings],
          classOf[SQLSchedulingSettings]
        ))) + KafkaSourceSettingsSerializers.KafkaSourceSettingsSerializer


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
    val fileContent = try EnvironmentVariableResolver.resolveFileContent(source.mkString) finally source.close()
    val mappingJob = org.json4s.jackson.JsonMethods.parse(fileContent).extract[FhirMappingJob]
    // check there are no duplicate name on mappingTasks of the job
    val duplicateMappingTasks = FhirMappingJobFormatter.findDuplicateMappingTaskNames(mappingJob.mappings)
    if (duplicateMappingTasks.nonEmpty) {
      throw new MappingException(s"Duplicate 'name' fields detected in the MappingTasks of the MappingJob: ${duplicateMappingTasks.mkString(", ")}. Please ensure that each MappingTask has a unique name.")
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
