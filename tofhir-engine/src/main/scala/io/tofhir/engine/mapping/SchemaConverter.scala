package io.tofhir.engine.mapping

import io.onfhir.api.parsers.IFhirFoundationResourceParser
import io.onfhir.api.validation.{ConstraintKeys, ElementRestrictions, ProfileRestrictions}
import io.onfhir.api.{FHIR_DATA_TYPES, FHIR_ROOT_URL_FOR_DEFINITIONS, Resource}
import io.onfhir.r4.parsers.R4Parser
import io.onfhir.validation.{ArrayRestriction, CardinalityMinRestriction, TypeRestriction}
import io.tofhir.common.model.{DataTypeWithProfiles, SimpleStructureDefinition}
import io.tofhir.engine.model.FhirMappingException
import org.apache.spark.sql.types._

/**
 * Utility class to convert schema representations in [[Resource]] format into Spark's [[StructType]]
 *
 * @param majorFhirVersion FHIR version to initialize the underlying FHIR parser
 */
class SchemaConverter(majorFhirVersion: String) {
  /**
   * Converts the FHIR schema as a [[Resource]] into Spark [[StructType]]
   *
   * @param schemaInJson
   * @return
   */
  def convertSchema(schemaInJson: Resource): StructType = {
    val profileRestrictions: ProfileRestrictions = getFoundationResourceParser().parseStructureDefinition(schemaInJson)
    val typee = convertToSparkSchema(profileRestrictions)
    typee
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

  private def getFoundationResourceParser(): IFhirFoundationResourceParser = {
    majorFhirVersion match {
      case "R4" => new R4Parser()
      case _ => throw new NotImplementedError()
    }
  }

  /**
   * Convert Spark data types to fhir data types and create a Schema.
   * @param structField Spark column metadata
   * @param defaultName Default name for path field
   * @return SimpleStructureDefinition object that defines a Schema
   */
  def fieldsToSchema(structField: StructField, defaultName: String): SimpleStructureDefinition = {
    SimpleStructureDefinition(
      id = structField.name,
      path = defaultName + "." + structField.name,
      dataTypes = structField.dataType match {
        case DataTypes.ShortType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))))
        case DataTypes.LongType => Some(Seq(DataTypeWithProfiles(dataType = "integer64", profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/integer64")))))
        case DataTypes.StringType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))))
        case DataTypes.IntegerType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))))
        case DataTypes.ByteType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))))
        case DataTypes.BinaryType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.BASE64BINARY, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.BASE64BINARY}")))))
        case DataTypes.BooleanType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.BOOLEAN, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.BOOLEAN}")))))
        case DataTypes.CalendarIntervalType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))))
        case DataTypes.DateType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATE}")))))
        case DataTypes.DoubleType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DECIMAL, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DECIMAL}")))))
        case DataTypes.FloatType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DECIMAL, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DECIMAL}")))))
        case DataTypes.NullType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))))
        case DataTypes.TimestampType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))))
      },
      isPrimitive = true,
      isChoiceRoot = false,
      isArray = false,
      minCardinality = 0,
      maxCardinality = Some(1),
      boundToValueSet = None,
      isValueSetBindingRequired = None,
      referencableProfiles = None,
      constraintDefinitions = None,
      sliceDefinition = None,
      sliceName = None,
      fixedValue = None,
      patternValue = None,
      referringTo = None,
      short = None,
      definition = None,
      comment = None,
      elements = None
    )
  }

}
