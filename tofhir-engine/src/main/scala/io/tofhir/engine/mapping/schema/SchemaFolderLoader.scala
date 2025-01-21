package io.tofhir.engine.mapping.schema

import com.typesafe.scalalogging.Logger
import io.onfhir.api.Resource
import io.onfhir.api.util.{FHIRUtil, IOUtil}
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.model.exception.FhirMappingException
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.engine.util.MajorFhirVersion
import org.apache.spark.sql.types.StructType

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import scala.io.Source

/**
 * Mapping source schema (StructureDefinition) repository from folder contents
 *
 * @param folderUri URI of the folder
 */
class SchemaFolderLoader(folderUri: URI, majorFhirVersion: String = MajorFhirVersion.R4) extends IFhirSchemaLoader {
  private val logger: Logger = Logger(this.getClass)

  private val schemas: Map[String, Resource] = loadSchemas()

  override def getSchema(schemaUrl: String): Option[StructType] = {
    schemas.get(schemaUrl).map(schemaInJson => new SchemaConverter(majorFhirVersion).convertSchema(schemaInJson))
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
      IOUtil.getFilesFromFolder(folder, recursively = true, ignoreHidden = true, withExtension = Some(FileExtensions.JSON.toString))
    } catch {
      case e: Throwable => throw FhirMappingException(s"Given folder for the schema repository is not valid.", e)
    }
  }


}
