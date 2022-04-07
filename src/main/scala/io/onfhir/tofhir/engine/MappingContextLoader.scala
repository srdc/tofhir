package io.onfhir.tofhir.engine

import com.fasterxml.jackson.databind.MappingIterator
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema}
import com.typesafe.scalalogging.Logger
import io.onfhir.tofhir.model.{ConceptMapContext, FhirMappingContext, FhirMappingContextCategories, FhirMappingContextDefinition, UnitConversionContext}

import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.javaapi.CollectionConverters

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

  def retrieveContext(contextDefinition: FhirMappingContextDefinition): Future[FhirMappingContext] = {
    if (contextDefinition.url.isDefined) {
      logger.debug("The context definition for the mapping repository is defined at a URL:{}. It will be loaded...", contextDefinition.url.get)
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
      val csvFile = new File(filePath)
      val csvMapper = new CsvMapper()
      val csvSchema = CsvSchema.emptySchema().withHeader()

      val javaList: java.util.List[java.util.Map[String, String]] =
        csvMapper.readerFor(classOf[java.util.Map[String, String]]) // read each line into a Map[String, String]
          .`with`(csvSchema) // where the key of the map will be the column name according to the first (header) row
          .readValues(csvFile)
          .readAll() // Read all lines as a List of Map
      CollectionConverters.asScala(javaList)
        .toSeq // convert the outer List to Scala Seq
        .map(CollectionConverters.asScala(_).toMap) // convert each inner Java Map ti Scala Map
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
      val (firstColumnName, _) = records.head.head // Get the first element in the records list and then get the first (k,v) pair to get the name of the first column.
      records.foldLeft(Map[String, Map[String, String]]()) { (conceptMap, columnMap) =>
        conceptMap + (columnMap(firstColumnName)-> columnMap)
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
