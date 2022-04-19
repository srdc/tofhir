package io.onfhir.tofhir.engine

import com.typesafe.scalalogging.Logger
import io.onfhir.tofhir.model.{FhirMapping, FhirMappingContext, FhirMappingContextDefinition, FhirMappingException}

import java.io.File
import scala.io.Source
import io.onfhir.util.JsonFormatter._

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import scala.util.Try

/**
 * Repository that keeps all mappings and other data in folder
 *
 * @param folderPath Path to the folder
 */
class FhirMappingFolderRepository(folderUri: URI) extends IFhirMappingRepository {
  private val logger: Logger = Logger(this.getClass)

  private val fhirMappings: Map[String, FhirMapping] = loadMappings()

  /**
   * Get the list of JSON files (ending with .json) from the folder repository.
   *
   * @return
   */
  private def getJsonFiles: List[File] = {
    val folder = new File(folderUri)
    if (folder.exists && folder.isDirectory) {
      folder.listFiles().toList.filter(_.getName.endsWith(".json"))
    } else {
      throw FhirMappingException(s"Given folder for the folder repository is not valid. URI: $folderUri")
    }
  }

  /**
   * Return the FhirMapping files among the JSON files in the folder repository by testing with JSON parsing against #FhirMapping.
   *
   * @return
   */
  private def getFhirMappings: List[FhirMapping] = {
    val files = getJsonFiles
    files.map { f =>
      val source = Source.fromFile(f, StandardCharsets.UTF_8.name()) // read the JSON file
      val fileContent = try source.mkString finally source.close()
      Try(fileContent.parseJson.extract[FhirMapping]).toOption
    }.filter(_.nonEmpty) // Remove the elements from the list if they are not valid FhirMapping JSONs
      .map(_.get) // Get rid of the Option
  }

  /**
   * Given a list of #FhirMapping, normalize the URIs pointing to the context definition files (e.g., concept mappings)
   * in the #FhirMappingContextDefinition objects with respect to the folderPath of this repository because the paths
   * may be given as relative paths in those URIs.
   *
   * @param fhirMappings
   * @return
   */
  private def normalizeContextURLs(fhirMappings: List[FhirMapping]): List[FhirMapping] = {
    fhirMappings.map { fhirMapping => // iterate over each FhirMapping
      val newContextDefinitionMap = fhirMapping.context.map { case (key, contextDefinition) => // iterate over each contextDefinition entry
        val newContextDefinition = // create a new context definition object
          if(contextDefinition.url.isDefined) {
            val folderPathObj = Paths.get(folderUri)
            val contextDefinitionPath = Paths.get(contextDefinition.url.get) // parse the path
            if(!contextDefinitionPath.isAbsolute) { //if the URL of the context definition object is relative
              contextDefinition.withURL(Paths.get(folderPathObj.normalize().toString, contextDefinitionPath.normalize().toString).toAbsolutePath.toString) // join it with the folderPath of this repository
            } else contextDefinition // keep it otherwise
          } else contextDefinition // keep it otherwise
        key -> newContextDefinition
      }
      fhirMapping.withContext(newContextDefinitionMap)
    }
  }

  /**
   * Load the mappings from the folder
   *
   * @return
   */
  private def loadMappings(): Map[String, FhirMapping] = {
    logger.debug("Loading all mappings from folder:{}", folderUri)
    val mappings = normalizeContextURLs(getFhirMappings)
      .foldLeft(Map[String, FhirMapping]()) { (map, fhirMapping) => map + (fhirMapping.url -> fhirMapping) }
    logger.debug("{} mappings were loaded from the mapping folder:{}", mappings.size, folderUri)
    logger.debug("Loaded mappings are:{}{}", System.lineSeparator(), mappings.keySet.mkString(System.lineSeparator()))
    mappings
  }

  /**
   * Return the Fhir mapping definition by given url
   *
   * @param mappingUrl Fhir mapping url
   * @return
   */
  override def getFhirMappingByUrl(mappingUrl: String): FhirMapping = {
    try {
      fhirMappings(mappingUrl)
    } catch {
      case e: NoSuchElementException => throw FhirMappingException(s"FhirMapping with url $mappingUrl cannot be found in folder $folderUri", e)
      case e: Throwable => throw FhirMappingException(s"Unknown exception while retrieving the FhirMapping with url $mappingUrl from folder $folderUri", e)
    }
  }

}
