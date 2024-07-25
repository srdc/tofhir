package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.common.model.SchemaDefinition
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.endpoint.SchemaDefinitionEndpoint.{SEGMENT_IMPORT, SEGMENT_IMPORT_ZIP, SEGMENT_INFER, SEGMENT_REDCAP, SEGMENT_SCHEMAS}
import io.tofhir.common.model.Json4sSupport._
import io.tofhir.server.model.{ImportSchemaSettings, InferTask}
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.common.model.{BadRequest,InternalError, ResourceNotFound, ToFhirRestCall}
import io.tofhir.server.endpoint.MappingContextEndpoint.ATTACHMENT
import io.onfhir.api.Resource
import io.tofhir.server.repository.mapping.IMappingRepository
import io.tofhir.server.repository.schema.ISchemaRepository
import io.tofhir.server.service.SchemaDefinitionService
import akka.http.scaladsl.model.{HttpEntity, HttpResponse}
import akka.http.scaladsl.server.directives.FileInfo
import io.onfhir.exception.InitializationException
import io.onfhir.util.OnFhirZipInputStream
import org.apache.commons.io.input.BOMInputStream
import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods

import java.io.{File, InputStreamReader, Reader}
import java.nio.file.{Files, Path}
import java.util.zip.ZipEntry
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.collection.mutable

class SchemaDefinitionEndpoint(schemaRepository: ISchemaRepository, mappingRepository: IMappingRepository) extends LazyLogging {

  val service: SchemaDefinitionService = new SchemaDefinitionService(schemaRepository, mappingRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_SCHEMAS) {
      val projectId: String = request.projectId.get
      pathEndOrSingleSlash {
        parameterMap { queryParams =>
          queryParams.get("url") match {
            case Some(url) => getSchemaByUrl(projectId, url)
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
      } ~ pathPrefix(Segment) { id: String => // Operations on a single schema identified by its id
        getSchema(projectId, id) ~ updateSchema(projectId, id) ~ deleteSchema(projectId, id)
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

  private def getSchema(projectId: String, id: String): Route = {
    get {
      parameterMap { queryParams =>
        complete {
          // Requested format of the schema: "StructureDefinition" or "SimpleStructureDefinition"
          val format: String = queryParams.getOrElse("format", SchemaFormats.SIMPLE_STRUCTURE_DEFINITION)
          // Send structure definition for the user to export
          if(format == SchemaFormats.STRUCTURE_DEFINITION){
            service.getSchemaAsStructureDefinition(projectId, id) map {
              case Some(schemaStructureDefinition) => StatusCodes.OK -> schemaStructureDefinition
              case None => {
                throw ResourceNotFound("Schema not found", s"Schema definition with name $id not found")
              }
            }
          }
            // Send simple structure definition for general use in frontend
          else {
            service.getSchema(projectId, id) map {
              case Some(schemaSimpleStructureDefinition) => StatusCodes.OK -> schemaSimpleStructureDefinition
              case None => StatusCodes.NotFound -> {
                throw ResourceNotFound("Schema not found", s"Schema definition with name $id not found")
              }
            }
          }
        }
      }
    }
  }

  private def getSchemaByUrl(projectId: String, url: String): Route = {
    get {
      complete {
        service.getSchemaByUrl(projectId, url) map {
          case Some(schemaDefinition) => StatusCodes.OK -> schemaDefinition
          case None => StatusCodes.NotFound -> {
            throw ResourceNotFound("Schema not found", s"Schema definition with url $url not found")
          }
        }
      }
    }
  }

  private def updateSchema(projectId: String, id: String): Route = {
    put {
      entity(as[SchemaDefinition]) { schemaDefinition =>
        complete {
          service.putSchema(projectId, id, schemaDefinition) map { _ =>
            StatusCodes.OK -> schemaDefinition
          }
        }
      }
    }
  }

  private def deleteSchema(projectId: String, id: String): Route = {
    delete {
      complete {
        service.deleteSchema(projectId, id) map { _ =>
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
          parameters("rootUrl") { rootUrl =>
            complete {
              service.importREDCapDataDictionary(projectId, byteSource, rootUrl)
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
          val zipProcessingResult = processZipFile(file.toPath)
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

  /**
   * Processes a ZIP file containing FHIR structure definitions and extracts the resources.
   *
   * This method reads the ZIP file, parses each JSON resource, and collects them into a sequence of
   * `Resource` objects.
   *
   * @param zipFilePath The path to the ZIP file to be processed.
   * @return A Future containing a sequence of `Resource` objects extracted from the ZIP file.
   */
  private def processZipFile(zipFilePath: Path): Future[Seq[Resource]] = Future {
    /**
     * Parses a JSON resource from the provided reader.
     *
     * This method attempts to parse a JSON resource from the given `Reader`. If the path does not end with ".json",
     * or if the JSON parsing fails, appropriate exceptions are thrown.
     *
     * @param reader The reader to read the JSON data from.
     * @param path The file path from which the resource is being read.
     * @return The parsed `Resource` object from the JSON data.
     *
     * @throws InternalError If there is a problem parsing the JSON data from the path.
     * @throws BadRequest If the file path does not end with ".json".
     */
    def parseResource(reader: Reader, path: String): Resource = {
      if (path.endsWith(".json"))
        try {
          JsonMethods.parse(reader).asInstanceOf[JObject]
        }
        catch {
          case e: Exception =>
            throw InternalError("JSON parsing problem",s"Cannot parse resource from path $path!", Some(e))
        }
      else
        throw BadRequest("Invalid JSON!",s"Cannot read resource from path $path, it should be JSON file!")
    }

    val zipStream = new OnFhirZipInputStream(Files.newInputStream(zipFilePath))
    val resources: mutable.ListBuffer[Resource] = new mutable.ListBuffer[Resource]
    var zipEntry: ZipEntry = zipStream.getNextEntry

    while (zipEntry != null) {
      val reader = new InputStreamReader(BOMInputStream.builder.setInputStream(zipStream).get(), "UTF-8")
      resources.append(parseResource(reader, zipEntry.getName))
      zipStream.closeEntry()
      zipEntry = zipStream.getNextEntry
    }
    resources.toSeq
  }
}

object SchemaDefinitionEndpoint {
  val SEGMENT_SCHEMAS = "schemas"
  val SEGMENT_INFER = "infer"
  val SEGMENT_REDCAP = "redcap"
  val SEGMENT_IMPORT = "import"
  val SEGMENT_IMPORT_ZIP = "import-zip"
}

/**
 * The schema formats available for POST and GET schema methods
 */
object SchemaFormats{
  val STRUCTURE_DEFINITION = "StructureDefinition"
  val SIMPLE_STRUCTURE_DEFINITION = "SimpleStructureDefinition"
}

