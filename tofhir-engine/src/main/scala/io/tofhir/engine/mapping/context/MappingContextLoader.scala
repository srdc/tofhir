package io.tofhir.engine.mapping.context

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.model._
import io.tofhir.engine.util.{CsvUtil, FileUtils}

import java.io.File
import java.nio.file.Paths
import io.tofhir.engine.Execution.actorSystem.dispatcher
import scala.concurrent.Future

/**
 * Interface to load a context from definition
 */
trait IMappingContextLoader {
  /**
   * Retrieve the given context data from the definition
   *
   * @param contextDefinition Mapping context definition
   * @return
   */
  def retrieveContext(contextDefinition: FhirMappingContextDefinition): Future[FhirMappingContext]
}

class MappingContextLoader extends IMappingContextLoader {

  def retrieveContext(contextDefinition: FhirMappingContextDefinition): Future[FhirMappingContext] = {
    if (contextDefinition.url.isDefined) {
      //logger.debug("The context definition for the mapping repository is defined at a URL:{}. It will be loaded...", contextDefinition.url.get)
      // FIXME: To build the context, we only accept CSV files with a header to read from.
      contextDefinition.category match {
        case FhirMappingContextCategories.CONCEPT_MAP => readConceptMapContextFromCSV(contextDefinition.url.get).map { concepts => ConceptMapContext(concepts) }
        case FhirMappingContextCategories.UNIT_CONVERSION_FUNCTIONS => readUnitConversionFunctionsFromCSV(contextDefinition.url.get).map { conversionFunctions => UnitConversionContext(conversionFunctions) }
      }
    } else {
      // FIXME: If there is no URL to read from, then the context definition may be given through the value of the FhirMappingContextDefinition as a JSON object.
      //  It needs to be converted to a FhirMappingContext object.
      throw new NotImplementedError("A FhirMappingContextDefinition must include a valid URL pointing to a CSV file so that I can load it.")
    }
  }

  /**
   * Read the CSV file from the given filePath and return a Sequence where each element is a Map[column_name -> value).
   * It handles {@link FhirMappingContextUrlPlaceHolder.CONTEXT_REPO} placeholder in the given filePath by replacing it
   * with the mapping context repository folder path.
   *
   * @param filePath
   * @return
   */
  private def readFromCSV(filePath: String): Future[(Seq[String],Seq[Map[String, String]])] = {
    val path =
    // replace $CONTEXT_REPO placeholder
    if (filePath.contains(FhirMappingContextUrlPlaceHolder.CONTEXT_REPO))
      filePath.replace(FhirMappingContextUrlPlaceHolder.CONTEXT_REPO,
        ToFhirConfig.engineConfig.mappingContextRepositoryFolderPath)
     else
      filePath
    Future {
      CsvUtil.readFromCSVAndReturnWithColumnNames(FileUtils.getPath(path).toString)
    }
  }

  /**
   * Read concept mappings from the given CSV file.
   * Example dataset to understand what this function does:
   * Input CSV content (concept mappings), assumed to be at some file path specified:
   * -----------------------------
   * source_code,target_code,display_value
   * 001,A1,Foo
   * 001,A2,Bar
   * 002,B1,Baz
   * -----------------------------
   *
   * Explanation of this structure:
   * - "source_code" is the key (the first column header), which will group the rows.
   * - "target_code" and "display_value" are part of the data for each key grouping.
   *
   * Expected output of processing:
   * Map(
   *   "001" -> Seq(
   *     Map("source_code" -> "001", "target_code" -> "A1", "display_value" -> "Foo"),
   *     Map("source_code" -> "001", "target_code" -> "A2", "display_value" -> "Bar")
   *   ),
   *   "002" -> Seq(
   *     Map("source_code" -> "002", "target_code" -> "B1", "display_value" -> "Baz")
   *   )
   * )
   *
   * @param filePath
   * @return
   */
  private def readConceptMapContextFromCSV(filePath: String): Future[Map[String, Seq[Map[String, String]]]] = {
    readFromCSV(filePath) map {
      case (columns, records) =>
        //val (firstColumnName, _) = records.head.head // Get the first element in the records list and then get the first (k,v) pair to get the name of the first column.
        val columnHeadKey = columns.head
        records.foldLeft(Map[String, Seq[Map[String, String]]]()) { (conceptMap, columnMap) =>
          val key = columnMap(columnHeadKey)
          // If a source code has not been encountered before, add it as the first element.
          // Otherwise, append the new target values to the existing sequence.
          conceptMap.updatedWith(key) {
            case Some(existingValues) => Some(existingValues :+ columnMap)
            case None => Some(Seq(columnMap))
          }
        }
    }
  }

  /**
   * Read the unit conversions from the given CSV file.
   *
   * source_code,source_unit,target_unit,conversion_function
   * 1552,g/l,g/dL,"""$this * 0.1"""
   *
   * @param filePath
   * @return
   */
  private def readUnitConversionFunctionsFromCSV(filePath: String): Future[Map[(String, String), (String, String)]] = {
    readFromCSV(filePath) map {
      case (columnNames, records) =>
        val sourceCode = columnNames.head
        val sourceUnit = columnNames(1)
        val targetUnit = columnNames(2)
        val conversionFunction = columnNames(3)
        records.foldLeft(Map[(String, String), (String, String)]()) { (unitConversionMap, columnMap) =>
          unitConversionMap + ((columnMap(sourceCode) -> columnMap(sourceUnit)) -> (columnMap(targetUnit) -> columnMap(conversionFunction)))
        }
    }
  }

}

object MappingContextLoader {

  /**
   * Given a sequence of (#FhirMapping, File) tuples, normalize the URIs pointing to the context definition files (e.g., concept mappings)
   * in the #FhirMappingContextDefinition objects with respect to the file path of the FhirMapping because the paths
   * may be given as relative paths in those URIs within the mapping definitions.
   *
   * @param fhirMappings
   * @return
   */
  def normalizeContextURLs(fhirMappings: Seq[(FhirMapping, File)]): Seq[FhirMapping] = {
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
}
