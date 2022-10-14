package io.tofhir.engine.mapping

import com.typesafe.scalalogging.Logger
import io.tofhir.engine.model.{ConceptMapContext, FhirMappingContext, FhirMappingContextCategories, FhirMappingContextDefinition, UnitConversionContext}
import io.tofhir.engine.util.CsvUtil

import scala.concurrent.ExecutionContext.Implicits.global
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

class MappingContextLoader(fhirMappingRepository: IFhirMappingRepository) extends IMappingContextLoader {

  private val logger: Logger = Logger(this.getClass)

  final val CONCEPT_MAP_FIRST_COLUMN_NAME = "source_code"

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
   * Read the CSV file from the given filePath and return a Sequence where each element is a Map[column_name -> value)
   *
   * @param filePath
   * @return
   */
  private def readFromCSV(filePath: String): Future[Seq[Map[String, String]]] = {
    Future {
      CsvUtil.readFromCSV(filePath)
    }
  }

  /**
   * Read concept mappings from the given CSV file.
   *
   * @param filePath
   * @return
   */
  private def readConceptMapContextFromCSV(filePath: String): Future[Map[String, Map[String, String]]] = {
    readFromCSV(filePath) map { records =>
      //val (firstColumnName, _) = records.head.head // Get the first element in the records list and then get the first (k,v) pair to get the name of the first column.
      records.foldLeft(Map[String, Map[String, String]]()) { (conceptMap, columnMap) =>
        conceptMap + (columnMap(CONCEPT_MAP_FIRST_COLUMN_NAME)-> columnMap)
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
    readFromCSV(filePath) map { records =>
      val columnNames = records.head.keys.toSeq
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
