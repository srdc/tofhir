package io.onfhir.tofhir.util

import io.onfhir.tofhir.config.ErrorHandlingType
import io.onfhir.tofhir.model.{BasicAuthenticationSettings, BearerTokenAuthorizationSettings, FhirMappingJob, FhirRepositorySinkSettings, FileSystemSinkSettings, FileSystemSource, FileSystemSourceSettings, KafkaSource, KafkaSourceSettings, LocalFhirTerminologyServiceSettings, SqlSource, SqlSourceSettings}
import org.json4s.{Formats, ShortTypeHints}
import org.json4s.ext.EnumNameSerializer
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
          //Source settings
          classOf[FileSystemSourceSettings],
          classOf[SqlSourceSettings],
          classOf[KafkaSourceSettings],
          // Authorization types
          classOf[BearerTokenAuthorizationSettings],
          classOf[BasicAuthenticationSettings],
          //Terminology setvices
          classOf[LocalFhirTerminologyServiceSettings]
        ))) +
      new EnumNameSerializer(ErrorHandlingType)


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
    val fileContent = try source.mkString finally source.close()
    org.json4s.jackson.JsonMethods.parse(fileContent).extract[FhirMappingJob]
  }
}
