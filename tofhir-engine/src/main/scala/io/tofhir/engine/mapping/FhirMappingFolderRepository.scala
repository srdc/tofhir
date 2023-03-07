package io.tofhir.engine.mapping

import com.typesafe.scalalogging.Logger
import io.onfhir.api.util.IOUtil
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.model.{FhirMapping, FhirMappingContextUrlPlaceHolder, FhirMappingException}
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import org.json4s._

import java.io.File
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import scala.io.Source

/**
 * Repository that keeps all mappings and other data in folder
 *
 * @param folderUri Path to the folder
 */
class FhirMappingFolderRepository(folderUri: URI) extends IFhirMappingRepository {
  private val logger: Logger = Logger(this.getClass)

  private val fhirMappings: Map[String, FhirMapping] = loadMappings()

  /**
   * Return the FhirMapping files among the JSON files in the folder repository by testing with JSON parsing against #FhirMapping.
   *
   * @return a sequence of tuples (FhirMapping, File) where the File is pointing to the FhirMapping.
   */
  private def getFhirMappings: Seq[(FhirMapping, File)] = {
    val folder = new File(folderUri)
    var files = Seq.empty[File]
    try {
      files = IOUtil.getFilesFromFolder(folder, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
    } catch {
      case e: Throwable => throw FhirMappingException(s"Given folder for the mapping repository is not valid.", e)
    }
    files.map { f =>
      val source = Source.fromFile(f, StandardCharsets.UTF_8.name()) // read the JSON file
      val fileContent = try source.mkString finally source.close()
      val fhirMapping = try {
        fileContent.parseJson.removeField { // Remove any fields starting with @ from the JSON.
          case JField(fieldName, _) if fieldName.startsWith("@") => true
          case _ => false
        }.extractOpt[FhirMapping]
      } catch {
        case e: Exception =>
          logger.error(s"Cannot parse the mapping file ${f.getAbsolutePath}.")
          Option.empty[FhirMapping]
      }
      fhirMapping -> f
    }.filter(_._1.nonEmpty) // Remove the elements from the list if they are not valid FhirMapping JSONs
      .map { case (fm, file) => fm.get -> file } // Get rid of the Option
  }

  /**
   * Given a sequence of (#FhirMapping, File) tuples, normalize the URIs pointing to the context definition files (e.g., concept mappings)
   * in the #FhirMappingContextDefinition objects with respect to the file path of the FhirMapping because the paths
   * may be given as relative paths in those URIs within the mapping definitions.
   *
   * @param fhirMappings
   * @return
   */
  private def normalizeContextURLs(fhirMappings: Seq[(FhirMapping, File)]): Seq[FhirMapping] = {
    fhirMappings.map { case (fhirMapping, file) => // iterate over each FhirMapping
      val newContextDefinitionMap = fhirMapping.context.map { case (key, contextDefinition) => // iterate over each contextDefinition entry
        val newContextDefinition = // create a new context definition object
          if (contextDefinition.url.isDefined) {
            if (contextDefinition.url.get.contains(FhirMappingContextUrlPlaceHolder.CONTEXT_REPO)) { // if the URL of the context definition object contains the $CONTEXT_REPO placeholder
              val replacedContextDefinition = contextDefinition.url.get.replace(FhirMappingContextUrlPlaceHolder.CONTEXT_REPO,
                ToFhirConfig.engineConfig.mappingContextRepositoryFolderPath)
              contextDefinition.withURL(FileUtils.getPath(replacedContextDefinition).toAbsolutePath.toString)
            } else { // context path is relative to the mapping file
              val folderPathObj = Paths.get(file.getParent)
              val contextDefinitionPath = Paths.get(contextDefinition.url.get) // parse the path
              if (!contextDefinitionPath.isAbsolute) { //if the URL of the context definition object is relative
                contextDefinition.withURL(Paths.get(folderPathObj.normalize().toString, contextDefinitionPath.normalize().toString).toAbsolutePath.toString) // join it with the folderPath of this repository
              } else contextDefinition // keep it otherwise
            }
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
    //logger.debug("Loading all mappings from folder:{}", folderUri)
    val mappings = normalizeContextURLs(getFhirMappings)
      .foldLeft(Map[String, FhirMapping]()) { (map, fhirMapping) =>
        if (map.contains(fhirMapping.url)) {
          val msg = s"Multiple mapping definitions with the same URL: ${fhirMapping.url}. URLs must be unique."
          logger.error(msg)
          throw new IllegalStateException(msg)
        }
        map + (fhirMapping.url -> fhirMapping)
      }
    //logger.debug("{} mappings were loaded from the mapping folder:{}", mappings.size, folderUri)
    //logger.debug("Loaded mappings are:{}{}", System.lineSeparator(), mappings.keySet.mkString(System.lineSeparator()))
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
