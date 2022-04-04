package io.onfhir.tofhir.engine

import com.typesafe.scalalogging.Logger
import io.onfhir.tofhir.model.{FhirMapping, FhirMappingContext, FhirMappingException}

import java.io.File
import scala.concurrent.Future
import scala.io.Source
import io.onfhir.util.JsonFormatter._

import scala.util.Try

/**
 * Repository that keeps all mappings and other data in folder
 * @param folderPath  Path to the folder
 */
class FhirMappingFolderRepository(folderPath:String) extends IFhirMappingRepository {
  private val logger: Logger = Logger(this.getClass)

  private val fhirMappings:Map[String, FhirMapping] = loadMappings()

  private def getJsonFiles: List[File] = {
    val folder = new File(folderPath)
    if(folder.exists && folder.isDirectory) {
      folder.listFiles().toList.filter(_.getName.endsWith(".json"))
    } else {
      throw FhirMappingException(s"Given folder for the folder repository is not valid. Path: $folderPath")
    }
  }

  private def getFhirMappings: List[FhirMapping] = {
    val files = getJsonFiles
    files.map { f =>
      val source = Source.fromFile(f, "utf-8") // read the JSON file
      val fileContent = try source.mkString finally source.close()
      Try(fileContent.parseJson.extract[FhirMapping]).toOption
    }.filter(_.nonEmpty) // Remove the elements from the list if they are not valid FhirMapping JSONs
      .map(_.get) // Get rid of the Option
  }

  /**
   * Load the mappings from the folder
   * @return
   */
  private def loadMappings():Map[String, FhirMapping] = {
    logger.debug("Loading all mappings from folder:{}", folderPath)
    val mappings = getFhirMappings.foldLeft(Map[String, FhirMapping]()) { (map, fhirMapping) => map + (fhirMapping.url -> fhirMapping)}
    logger.debug("{} mappings were loaded from the mapping folder:{}", mappings.size, folderPath)
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
    fhirMappings(mappingUrl)
  }

}
