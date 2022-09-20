package io.onfhir.tofhir.engine

import io.onfhir.api.{FHIR_DATA_TYPES, Resource}
import io.onfhir.api.parsers.IFhirFoundationResourceParser
import io.onfhir.api.validation.{ConstraintKeys, ElementRestrictions, ProfileRestrictions}
import io.onfhir.r4.parsers.R4Parser
import io.onfhir.tofhir.model.FhirMappingException
import io.onfhir.validation.{ArrayRestriction, CardinalityMinRestriction, TypeRestriction}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

/**
 * Base class to convert mapping source schemas given in FHIR StructureDefinitions to Spark schema
 *
 * @param majorFhirVersion
 */
abstract class AbstractFhirSchemaLoader(majorFhirVersion: String = "R4") extends IFhirSchemaLoader {
  /**
   * Load the schema from the url and return parsed JSON
   *
   * @param url
   * @return
   */
  def loadSchema(url: String): Option[Resource]

  /**
   *
   * @param schemaUrl URL of the schema
   * @return
   */
  def getSchema(schemaUrl: String): Option[StructType] = {
    loadSchema(schemaUrl) map { schemaInJson =>
      val profileRestrictions = getFoundationResourceParser().parseStructureDefinition(schemaInJson)
      convertToSparkSchema(profileRestrictions)
    }
  }

  /**
   * Convert simple (tabular) Json schema to Spark structs
   * TODO Handle this correctly for any FHIR StructureDefinition
   *
   * @param profileRestrictions
   * @return
   */
  protected def convertToSparkSchema(profileRestrictions: ProfileRestrictions): StructType = {
    val fields = profileRestrictions
      .elementRestrictions
      .map {
        case (elName, eRestrictions) =>
          StructField(
            elName,
            getSparkType(eRestrictions),
            isNullable(eRestrictions)
          )
      }
    StructType(fields)
  }

  /**
   * Check if field is nullable
   *
   * @param elementRestrictions
   * @return
   */
  private def isNullable(elementRestrictions: ElementRestrictions): Boolean = {
    elementRestrictions.restrictions.get(ConstraintKeys.MIN)
      .forall(_.asInstanceOf[CardinalityMinRestriction].n == 0)
  }

  /**
   * Convert simple FHIR type to Spark type
   *
   * @param elementRestrictions
   * @return
   */
  private def getSparkType(elementRestrictions: ElementRestrictions): DataType = {
    val isArray = elementRestrictions.restrictions.get(ConstraintKeys.ARRAY).exists(_.asInstanceOf[ArrayRestriction].isArray)

    val baseType =
      elementRestrictions
        .restrictions
        .get(ConstraintKeys.DATATYPE)
        .map(_.asInstanceOf[TypeRestriction])
        .flatMap(_.dataTypesAndProfiles.headOption.map(_._1)) match {
        case None => StringType
        case Some(fhirType) =>
          fhirType match {
            case FHIR_DATA_TYPES.ID | FHIR_DATA_TYPES.URI | FHIR_DATA_TYPES.URL | FHIR_DATA_TYPES.CODE |
                 FHIR_DATA_TYPES.STRING | FHIR_DATA_TYPES.OID | FHIR_DATA_TYPES.UUID | FHIR_DATA_TYPES.BASE64BINARY => StringType
            case FHIR_DATA_TYPES.DATE | FHIR_DATA_TYPES.TIME | FHIR_DATA_TYPES.DATETIME | FHIR_DATA_TYPES.INSTANT => StringType
            case FHIR_DATA_TYPES.DECIMAL => DoubleType
            case FHIR_DATA_TYPES.INTEGER | FHIR_DATA_TYPES.POSITIVEINT => IntegerType
            case FHIR_DATA_TYPES.UNSIGNEDINT => LongType
            case FHIR_DATA_TYPES.BOOLEAN => BooleanType
            case oth =>
              throw FhirMappingException(s"Given FHIR type $oth cannot be converted to Spark SQL types!")
          }
      }
    if (isArray)
      ArrayType(baseType)
    else
      baseType
  }

  protected def getFoundationResourceParser(): IFhirFoundationResourceParser = {
    majorFhirVersion match {
      case "R4" => new R4Parser()
      case _ => throw new NotImplementedError()
    }
  }

}
