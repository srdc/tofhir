package io.tofhir.engine.util

import io.tofhir.engine.model.{BasicAuthenticationSettings, BearerTokenAuthorizationSettings, FhirMappingJob, FhirMappingTask, FhirRepositorySinkSettings, FhirServerSource, FhirServerSourceSettings, FileSystemSinkSettings, FileSystemSource, FileSystemSourceSettings, FixedTokenAuthenticationSettings, KafkaSource, KafkaSourceSettings, LocalFhirTerminologyServiceSettings, SQLSchedulingSettings, SchedulingSettings, SqlSource, SqlSourceSettings}
import org.json4s.{Formats, MappingException, ShortTypeHints}
import org.json4s.jackson.Serialization

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
    val fileContent = try replaceEnvironmentVariables(source.mkString) finally source.close()
    val mappingJob = org.json4s.jackson.JsonMethods.parse(fileContent).extract[FhirMappingJob]
    // check there are no duplicate name on mappingTasks of the job
    val duplicateMappingTasks = FhirMappingJobFormatter.findDuplicateMappingTaskNames(mappingJob.mappings)
    if (duplicateMappingTasks.nonEmpty) {
      throw new MappingException(s"Duplicate 'name' fields detected in the MappingTasks of the MappingJob: ${duplicateMappingTasks.mkString(", ")}. Please ensure that each MappingTask has a unique name.")
    }
    mappingJob
  }

  private def replaceEnvironmentVariables(fileContent: String): String = {
    var returningContent = fileContent;
    //    val regex = """\$\{(.*?)\}""".r
    EnvironmentVariable.values.foreach { e =>
      val regex = "\\$\\{" + e.toString + "\\}"
      if (sys.env.contains(e.toString)) returningContent = returningContent.replaceAll(regex, sys.env(e.toString))
    }
    returningContent
  }

  object EnvironmentVariable extends Enumeration {
    type EnvironmentVariable = Value
    final val FHIR_REPO_URL = Value("FHIR_REPO_URL");
    final val DATA_FOLDER_PATH= Value("DATA_FOLDER_PATH");
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
