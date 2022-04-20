package io.onfhir.tofhir.engine

import com.typesafe.scalalogging.Logger
import io.onfhir.api.Resource
import io.onfhir.api.util.FHIRUtil
import io.onfhir.tofhir.model.FhirMappingException
import io.onfhir.util.JsonFormatter._

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
  override def loadSchema(url: String): Resource = {
    schemas.get(url) match {
      case Some(schema) => schema
      case None => throw FhirMappingException(s"Schema with url $url not found in folder $folderUri!")
    }
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
    if (folder.exists && folder.isDirectory) {
      folder.listFiles().toList.filter(_.getName.endsWith(".json"))
    } else {
      throw FhirMappingException(s"Given folder for the folder repository is not valid. URI: $folderUri")
    }
  }


}
