package io.tofhir.engine.mapping

import com.typesafe.scalalogging.Logger
import io.onfhir.api.Resource
import io.onfhir.api.util.{FHIRUtil, IOUtil}
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.model.FhirMappingException
import io.tofhir.engine.util.FileUtils.FileExtensions

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import scala.io.Source

/**
 * Mapping source schema (StructureDefinition) repository from folder contents
 *
 * @param folderUri URI of the folder
 */
class SchemaFolderRepository(folderUri: URI) extends AbstractFhirSchemaLoader {
  private val logger: Logger = Logger(this.getClass)

  private val schemas: Map[String, Resource] = loadSchemas()

  /**
   * Load the schema from the url and return parsed JSON
   *
   * @param url
   * @return
   */
  override def loadSchema(url: String): Option[Resource] = {
    schemas.get(url)
  }

  /**
   * Load all schemas in the folder
   *
   * @return
   */
  private def loadSchemas(): Map[String, Resource] = {
    val files = getListOfSchemas
    files
      .flatMap(f =>
        try {
          val source = Source.fromFile(f, StandardCharsets.UTF_8.name()) // read the JSON file
          val fileContent = try source.mkString finally source.close()
          val resource = fileContent.parseJson
          val url = FHIRUtil.extractValue[String](resource, "url")
          Some(url -> resource)
        } catch {
          case e: Throwable =>
            logger.error(s"Problem while reading schema file ${f.getName} from schema folder, skipping!", e)
            None
        }
      ).toMap
  }

  /**
   * Get the list of json files in the folder
   *
   * @return
   */
  private def getListOfSchemas: Seq[File] = {
    val folder = new File(folderUri)
    try {
      IOUtil.getFilesFromFolder(folder, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
    } catch {
      case e: Throwable => throw FhirMappingException(s"Given folder for the schema repository is not valid.", e)
    }
  }


}
