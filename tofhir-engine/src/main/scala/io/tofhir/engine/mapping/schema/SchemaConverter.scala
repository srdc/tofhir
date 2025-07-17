package io.tofhir.engine.mapping.schema

import io.onfhir.api.parsers.IFhirFoundationResourceParser
import io.onfhir.api.validation.{ConstraintKeys, ElementRestrictions, ProfileRestrictions}
import io.onfhir.api.{FHIR_DATA_TYPES, FHIR_ROOT_URL_FOR_DEFINITIONS, Resource}
import io.onfhir.r4.parsers.R4Parser
import io.onfhir.validation.{CardinalityMaxRestriction, CardinalityMinRestriction, TypeRestriction}
import io.onfhir.definitions.common.model.{DataTypeWithProfiles, SimpleStructureDefinition}
import io.tofhir.engine.util.MajorFhirVersion
import org.apache.spark.sql.types._

import java.sql.{Connection, DriverManager, ResultSetMetaData, SQLException, Types}
import scala.util.Using

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
    // If max = "*", it is removed from the restrictions by onFHIR.
    // Hence, we say that it is not an array if there is a cardinality restriction of <= 1.
    val notArray = elementRestrictions.restrictions.get(ConstraintKeys.MAX).exists {
      case m: CardinalityMaxRestriction => m.n <= 1
    }

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
            case FHIR_DATA_TYPES.DATE | FHIR_DATA_TYPES.TIME | FHIR_DATA_TYPES.DATETIME => StringType
            case FHIR_DATA_TYPES.DECIMAL => DoubleType
            case FHIR_DATA_TYPES.INTEGER | FHIR_DATA_TYPES.POSITIVEINT => IntegerType
            case FHIR_DATA_TYPES.INSTANT => TimestampType
            case FHIR_DATA_TYPES.UNSIGNEDINT => LongType
            case FHIR_DATA_TYPES.BOOLEAN => BooleanType
            // For complex data types, utilize StringType
            case _ => StringType
          }
      }

    if (notArray)
      baseType
    else
      ArrayType(baseType)
  }

  private def getFoundationResourceParser(): IFhirFoundationResourceParser = {
    majorFhirVersion match {
      case MajorFhirVersion.R4 => new R4Parser()
      case MajorFhirVersion.R5 => new R4Parser()
      case _ => throw new NotImplementedError()
    }
  }

  /**
   * Convert Spark data types to fhir data types and create a Schema.
   *
   * @param structField Spark column metadata
   * @param defaultName Default name for path field
   * @return SimpleStructureDefinition object that defines a Schema
   */
  def fieldsToSchema(structField: StructField, defaultName: String): SimpleStructureDefinition = {
    val (dataType, isArray, maxCardinality) = {
      def mapDataTypeToFhir(dataType: DataType): Option[Seq[DataTypeWithProfiles]] = {
        dataType match {
          case DataTypes.ShortType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))))
          case DataTypes.LongType => Some(Seq(DataTypeWithProfiles(dataType = "integer64", profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/integer64")))))
          case DataTypes.StringType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))))
          case DataTypes.IntegerType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))))
          case DataTypes.ByteType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))))
          case DataTypes.BinaryType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.BASE64BINARY, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.BASE64BINARY}")))))
          case DataTypes.BooleanType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.BOOLEAN, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.BOOLEAN}")))))
          case DataTypes.CalendarIntervalType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))))
          case DataTypes.DateType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATE}")))))
          case DataTypes.DoubleType | _: DecimalType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DECIMAL, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DECIMAL}")))))
          case DataTypes.FloatType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DECIMAL, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DECIMAL}")))))
          case DataTypes.NullType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))))
          // should we map TimestampType (Spark) to Instant (FHIR)? We map Instant to DateTime on line 91. This is a bit confusing.
          case DataTypes.TimestampType => Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))))
          case _ => None
        }
      }

      structField.dataType match {
        case arrayType: ArrayType =>
          (mapDataTypeToFhir(arrayType.elementType), true, None) // None represents "*" (unbounded)
        case other =>
          (mapDataTypeToFhir(other), false, Some(1))
      }
    }

    SimpleStructureDefinition(
      id = structField.name,
      path = defaultName + "." + structField.name,
      dataTypes = dataType,
      isPrimitive = true,
      isChoiceRoot = false,
      isArray = isArray,
      minCardinality = if (structField.nullable) 0 else 1,
      maxCardinality = maxCardinality,
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

  /**
   * Get the Spark schema from database metadata
   *
   * @param jdbcUrl
   * @param tableName
   * @param user
   * @param password
   * @return
   */
  def getTableSchema(jdbcUrl: String, user: String, password: String, sqlOrTable: String, isQuery: Boolean): StructType = {

    // Helper: splits a fully qualified table name into (schema, table)
    // If there's no dot, returns (null, tableName)
    def splitQualifiedTableName(tableName: String): (String, String) = {
      val parts = tableName.split("\\.", 2)
      if (parts.length == 2) (parts(0), parts(1)) else (null, tableName)
    }

    try {
      Using.Manager { use =>
        val connection = try {
          use(DriverManager.getConnection(jdbcUrl, user, password))
        } catch {
          case e: SQLException =>
            throw new RuntimeException(s"Failed to establish JDBC connection: ${e.getMessage}", e)
          case e: Throwable =>
            throw new RuntimeException(s"Unexpected error while establishing JDBC connection: ${e.getMessage}", e)
        }
        if (isQuery) {
          // For SQL queries, execute the query with a limit of zero rows
          val statement = use(connection.createStatement())
          val sqlWithLimit = sqlOrTable.replaceAll("(?i)\\s+LIMIT\\s+\\d+", "") + " LIMIT 0"
          try {
            val rs = use(statement.executeQuery(sqlWithLimit))
            val meta = rs.getMetaData
            val fields = (1 to meta.getColumnCount).map { i =>
              val columnName = meta.getColumnLabel(i)
              val sqlType = meta.getColumnType(i)
              val isNullable = meta.isNullable(i) != ResultSetMetaData.columnNoNulls
              StructField(columnName, mapSqlTypeToSpark(sqlType), nullable = isNullable)
            }
            StructType(fields)
          } catch {
            case e: SQLException =>
              throw new RuntimeException(s"Failed to execute SQL query for schema inference: ${e.getMessage}", e)
            case e: Throwable =>
              throw new RuntimeException(s"Unexpected error during schema inference: ${e.getMessage}", e)
          }
        } else {
          // For a table name, use JDBC metadata. If the table name is qualified,
          // split it into schema (or catalog) and table.
          val (dbSchema, dbTableName) = splitQualifiedTableName(sqlOrTable)
          // For a table name, use JDBC metadata to get the column definitions
          val metaData = connection.getMetaData
          val rs = use(metaData.getColumns(null, dbSchema, dbTableName, null))
          val fields = Iterator.continually(rs)
            .takeWhile(_.next())
            .map { rs =>
              val columnName = rs.getString("COLUMN_NAME")
              val sqlType = rs.getInt("DATA_TYPE")
              val isNullable = rs.getString("IS_NULLABLE") == "YES"
              StructField(columnName, mapSqlTypeToSpark(sqlType), nullable = isNullable)
            }
            .toSeq
          StructType(fields)
        }
      }.get
    }
  }

  /**
   * Map SQL data types to Spark data types for schema conversion
   *
   * @param sqlType
   * @return
   */
  def mapSqlTypeToSpark(sqlType: Int): DataType = sqlType match {
    case Types.VARCHAR | Types.LONGVARCHAR | Types.CHAR | Types.NCHAR | Types.NVARCHAR => StringType
    case Types.INTEGER | Types.TINYINT | Types.SMALLINT => IntegerType
    case Types.BIGINT => LongType
    case Types.FLOAT | Types.REAL => FloatType
    case Types.DOUBLE | Types.NUMERIC | Types.DECIMAL => DoubleType
    case Types.BOOLEAN | Types.BIT => BooleanType
    case Types.DATE => DateType
    case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE => TimestampType
    case Types.TIME | Types.TIME_WITH_TIMEZONE => TimestampType
    case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY => BinaryType
    case Types.ARRAY => ArrayType(StringType) // Default assumption
    case Types.JAVA_OBJECT | Types.STRUCT | Types.OTHER => StringType // Fallback for unknown types
    case _ => StringType // Default for unsupported types
  }

}
