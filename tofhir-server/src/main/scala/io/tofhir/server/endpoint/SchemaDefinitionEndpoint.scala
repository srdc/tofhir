package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.onfhir.definitions.common.model.SchemaDefinition
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.endpoint.SchemaDefinitionEndpoint.{SEGMENT_IMPORT, SEGMENT_IMPORT_ZIP, SEGMENT_INFER, SEGMENT_REDCAP, SEGMENT_SCHEMAS}
import io.onfhir.definitions.common.model.Json4sSupport._
import io.tofhir.server.model.{ImportSchemaSettings, InferTask}
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.common.model.{BadRequest, InternalError, ResourceNotFound, ToFhirRestCall}
import io.tofhir.server.endpoint.MappingContextEndpoint.ATTACHMENT
import io.onfhir.api.Resource
import io.tofhir.server.repository.mapping.IMappingRepository
import io.tofhir.server.repository.schema.ISchemaRepository
import io.tofhir.server.service.SchemaDefinitionService
import akka.http.scaladsl.server.directives.FileInfo
import io.tofhir.server.util.FileOperations

import java.io.File
import java.nio.file.Files
import scala.util.{Failure, Success}

class SchemaDefinitionEndpoint(schemaRepository: ISchemaRepository, mappingRepository: IMappingRepository) extends LazyLogging {

  val service: SchemaDefinitionService = new SchemaDefinitionService(schemaRepository, mappingRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_SCHEMAS) {
      val projectId: String = request.projectId.get
      pathEndOrSingleSlash {
        parameterMap { queryParams =>
          queryParams.get(SchemaDefinitionEndpoint.QUERY_PARAM_URL) match {
            case Some(url) => getSchemaByUrl(url, queryParams.get(SchemaDefinitionEndpoint.QUERY_PARAM_TYPE))
            case None => getAllSchemas(request) ~ createSchema(projectId, queryParams.getOrElse("format", SchemaFormats.SIMPLE_STRUCTURE_DEFINITION)) // Operations on all schemas
          }
        }
      } ~ pathPrefix(SEGMENT_INFER) { // infer a schema
        inferSchema()
      } ~ pathPrefix(SEGMENT_REDCAP) { // import a REDCap data dictionary file
        importREDCapDataDictionary(projectId)
      } ~ pathPrefix(SEGMENT_IMPORT) { // import a schema from a Fhir Server
        importFromFhirServer(projectId)
      } ~ pathPrefix(SEGMENT_IMPORT_ZIP) {
        importFromZipOfFHIRProfiles(projectId)
      } ~ pathPrefix(Segment) { schemaId: String => // Operations on a single schema identified by its id
        getSchema(projectId, schemaId) ~ updateSchema(projectId, schemaId) ~ deleteSchema(projectId, schemaId)
      }
    }
  }

  private def getAllSchemas(request: ToFhirRestCall): Route = {
    get {
      complete(service.getAllSchemas(request.projectId.get))
    }
  }

  /**
   * Create a new schema with the given body
   * @param projectId Id of the project in which the schemas will be created
   * @param format format of the schema in the request, there are two options StructureDefinition and SimpleStructureDefinition
   * @return the SchemaDefinition of the created schema
   */
  private def createSchema(projectId: String, format: String): Route = {
    post { // Create a new schema definition
      // If the schema is in the form of StructureDefinition, convert into SimpleStructureDefinition and save
      if (format == SchemaFormats.STRUCTURE_DEFINITION) {
        entity(as[Resource]) { schemaStructureDefinition =>
          complete {
            service.createSchemaFromStructureDefinition(projectId, schemaStructureDefinition)
          }
        }
      }
      else{
        entity(as[SchemaDefinition]) { schemaDefinition =>
          complete {
            service.createSchema(projectId, schemaDefinition) map { createdDefinition =>
              StatusCodes.Created -> createdDefinition
            }
          }
        }
      }
    }
  }

  private def getSchema(projectId: String, schemaId: String): Route = {
    get {
      parameterMap { queryParams =>
        complete {
          // Requested format of the schema: "StructureDefinition" or "SimpleStructureDefinition"
          val format: String = queryParams.getOrElse("format", SchemaFormats.SIMPLE_STRUCTURE_DEFINITION)
          // Send structure definition for the user to export
          if(format == SchemaFormats.STRUCTURE_DEFINITION){
            service.getSchemaAsStructureDefinition(projectId, schemaId) map {
              case Some(schemaStructureDefinition) => StatusCodes.OK -> schemaStructureDefinition
              case None => {
                throw ResourceNotFound("Schema not found", s"Schema definition with name $schemaId not found")
              }
            }
          }
            // Send simple structure definition for general use in frontend
          else {
            service.getSchema(projectId, schemaId) map {
              case Some(schemaSimpleStructureDefinition) => StatusCodes.OK -> schemaSimpleStructureDefinition
              case None => StatusCodes.NotFound -> {
                throw ResourceNotFound("Schema not found", s"Schema definition with name $schemaId not found")
              }
            }
          }
        }
      }
    }
  }

  private def getSchemaByUrl(url: String, returnType: Option[String]): Route = {
    get {
      complete {
        service.getSchemaByUrl(url) map {
          case Some(schemaDefinition) =>
            returnType match {
              case Some(SchemaFormats.SIMPLE_STRUCTURE_DEFINITION) =>
                // Return only the element definitions within the schema, excluding metadata such as name and description.
                // This is useful when only the element-level details are needed, without additional schema metadata.
                StatusCodes.OK -> schemaDefinition.fieldDefinitions.get
              case _ =>
                // Return the full schema definition, including all metadata and element definitions.
                StatusCodes.OK -> schemaDefinition
            }
          case None => StatusCodes.NotFound -> {
            throw ResourceNotFound("Schema not found", s"Schema definition with url $url not found")
          }
        }
      }
    }
  }

  private def updateSchema(projectId: String, schemaId: String): Route = {
    put {
      entity(as[SchemaDefinition]) { schemaDefinition =>
        complete {
          service.putSchema(projectId, schemaId, schemaDefinition) map { res: SchemaDefinition =>
            StatusCodes.OK -> res
          }
        }
      }
    }
  }

  private def deleteSchema(projectId: String, schemaId: String): Route = {
    delete {
      complete {
        service.deleteSchema(projectId, schemaId) map { _ =>
          StatusCodes.NoContent
        }
      }
    }
  }
  /**
   * Route to infer a schema
   * */
  private def inferSchema(): Route = {
    post {
      entity(as[InferTask]) { inferTask =>
        complete {
          service.inferSchema(inferTask) map {
            case Some(schemaDefinition) => StatusCodes.OK -> schemaDefinition
            case None => StatusCodes.BadRequest -> {
              throw BadRequest("Schema inferring problem", s"Schema cannot be inferred")
            }
          }
        }
      }
    }
  }

  /**
   * Route to import a REDCap data dictionary file which will be used to create schemas.
   * */
  private def importREDCapDataDictionary(projectId: String): Route = {
    post {
      fileUpload(ATTACHMENT) {
        case (_, byteSource) =>
          parameters("rootUrl", "recordIdField") { (rootUrl, recordIdField) =>
            complete {
              service.importREDCapDataDictionary(projectId, byteSource, rootUrl, recordIdField)
            }
          }

      }
    }
  }

  /**
   * Route to import a schema i.e. FHIR Structure Definition from the given FHIR Server.
   * */
  private def importFromFhirServer(projectId: String): Route = {
    post {
      entity(as[ImportSchemaSettings]) { importSchemaSettings =>
        complete {
          service.importSchema(projectId, importSchemaSettings)
        }
      }
    }
  }

  /**
   * Route to import FHIR profiles (structure definitions) from a ZIP file uploaded to the server.
   *
   * This route handles the uploading and processing of a ZIP file containing FHIR structure definitions.
   * It parses the ZIP file, validates and processes the JSON resources, and then creates schemas in the
   * specified project.
   *
   * @param projectId The ID of the project where the FHIR profiles will be imported.
   * @return A Route that handles POST requests to import the schema from the ZIP file.
   */
  private def importFromZipOfFHIRProfiles(projectId: String): Route = {
    post {
      storeUploadedFile("file", createTempFile) {
        case (_, file) =>
          val zipProcessingResult = FileOperations.processZipFile(file.toPath)
          onComplete(zipProcessingResult) {
            case Success(result) =>
              complete(
                service.createSchemas(projectId, result)
              )
            case Failure(ex) =>
              throw InternalError("Processing of ZIP file failed!", s"Cannot process the ZIP file: ${ex.getMessage}")
          }
      }
    }
  }

  /**
   * Creates a temporary file with the given file name and a ".tmp" extension.
   *
   * This method creates a temporary file in the system's default temporary-file directory.
   * The file is marked for deletion when the JVM exits, ensuring that it will be cleaned up automatically.
   *
   * @param fileInfo Information about the uploaded file, including the file name.
   * @return A temporary file object.
   */
  private def createTempFile(fileInfo: FileInfo): File = {
      val tempFile = Files.createTempFile(fileInfo.fileName, ".tmp").toFile
      tempFile.deleteOnExit()
      tempFile
  }
}

object SchemaDefinitionEndpoint {
  val SEGMENT_SCHEMAS = "schemas"
  val SEGMENT_INFER = "infer"
  val SEGMENT_REDCAP = "redcap"
  val SEGMENT_IMPORT = "import"
  val SEGMENT_IMPORT_ZIP = "import-zip"
  val QUERY_PARAM_TYPE = "type"
  val QUERY_PARAM_URL = "url"
}

/**
 * The schema formats available for POST and GET schema methods
 */
object SchemaFormats {
  /**
   * Specifies that the format of the schema to be created or retrieved follows the FHIR StructureDefinition resource format.
   * For more information on this format, see the FHIR StructureDefinition documentation at:
   * https://www.hl7.org/fhir/structuredefinition.html.
   */
  val STRUCTURE_DEFINITION = "StructureDefinition"

  /**
   * Specifies a format that retrieves only the element definitions of a schema, where each element is represented
   * by a {@link io.tofhir.common.model.SimpleStructureDefinition}. This format provides a simplified structure for the schema's elements.
   */
  val SIMPLE_STRUCTURE_DEFINITION = "SimpleStructureDefinition"
}

