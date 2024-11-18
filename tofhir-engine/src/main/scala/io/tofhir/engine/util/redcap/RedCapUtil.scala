package io.tofhir.engine.util.redcap

import io.onfhir.api.{FHIR_DATA_TYPES, FHIR_FOUNDATION_RESOURCES, FHIR_ROOT_URL_FOR_DEFINITIONS, Resource}
import io.onfhir.definitions.common.model.{DataTypeWithProfiles, SchemaDefinition, SimpleStructureDefinition}
import io.tofhir.common.util.SchemaUtil
import io.tofhir.engine.config.ToFhirConfig

import javax.ws.rs.BadRequestException

/**
 * Util class providing methods to extract schemas from a REDCap data dictionary.
 * */
object RedCapUtil {

  /**
   * Extracts the FHIR resources from the given REDCap data dictionary file.
   *
   * @param content           The content of a REDCap data dictionary file
   * @param definitionRootUrl The definition root url for the newly created schemas
   * @param recordIdField     The name of the field that represents the record ID.
   * @return the list of schemas extracted from REDCap data dictionary
   * */
  def extractSchemas(content: Seq[Map[String, String]], definitionRootUrl: String, recordIdField: String = ""): Seq[Resource] = {
    val schemaDefinitions: Seq[SchemaDefinition] = extractSchemasAsSchemaDefinitions(content, definitionRootUrl, recordIdField)
    // convert it to FHIR Resources
    schemaDefinitions.map(definition =>  SchemaUtil.convertToStructureDefinitionResource(definition, ToFhirConfig.engineConfig.schemaRepositoryFhirVersion))
  }

  /**
   * Extracts Schema Definitions from the given REDCap data dictionary file.
   *
   * @param content           The content of a REDCap data dictionary file
   * @param definitionRootUrl The definition root url for the newly created schemas
   * @param recordIdField     The name of the field that represents the record ID in the output.
   * @return the list of schemas extracted from REDCap data dictionary
   * @throws BadRequestException when REDCap data dictionary file does not include {@link RedCapDataDictionaryColumns.VARIABLE_FIELD_NAME}
   * */
  def extractSchemasAsSchemaDefinitions(content: Seq[Map[String, String]], definitionRootUrl: String, recordIdField: String): Seq[SchemaDefinition] = {
    // find forms
    val forms: Map[String, Seq[Map[String, String]]] = content.groupBy(row => row(RedCapDataDictionaryColumns.FORM_NAME))
    // create a schema for each form
    forms.map(form => {
      val schemaName = form._1
      val schemaId = schemaName.capitalize
      // get schema definitions
      var definitions:Seq[SimpleStructureDefinition] = Seq.empty
      // whether the record identifier field is included in the instrument or not
      var includeRecordIdentifierField: Boolean = false
      form._2.foreach(row => {
        // read columns
        // try to retrieve the variable name from the regular field name column.
        // if it's not present, fallback to the field name column that includes BOM.
        val variableName: Option[String] = row.get(RedCapDataDictionaryColumns.VARIABLE_FIELD_NAME).orElse(row.get(RedCapDataDictionaryColumns.VARIABLE_FIELD_NAME_WITH_BOM))
        if(variableName.isEmpty)
          throw new BadRequestException(s"Invalid REDCap Data Dictionary file since it does not include the following column: ${RedCapDataDictionaryColumns.VARIABLE_FIELD_NAME}")
        val fieldType = row(RedCapDataDictionaryColumns.FIELD_TYPE)
        val fieldLabel = row(RedCapDataDictionaryColumns.FIELD_LABEL).trim()
        val fieldNotes = row.getOrElse(RedCapDataDictionaryColumns.FIELD_NOTES, fieldLabel).trim()
        val required = row(RedCapDataDictionaryColumns.REQUIRED_FIELD)
        val textValidationType = row.get(RedCapDataDictionaryColumns.TEXT_VALIDATION_TYPE)

        // discard the descriptive fields since they do not provide any data and they are not included in the export
        // record response
        if(!fieldType.contentEquals(RedCapDataTypes.DESCRIPTIVE)){
          // find applicable data type
          val dataType = getDataType(fieldType, textValidationType)
          // find cardinality
          val cardinality = getCardinality(fieldType, required)

          definitions = definitions :+ SimpleStructureDefinition(id = variableName.get,
            path = s"$schemaId.${variableName.get}",
            dataTypes = Some(Seq(dataType)),
            isPrimitive = true, // every field of a REDCap schema is primitive
            isChoiceRoot = false,
            isArray = cardinality._2.isEmpty,
            minCardinality = cardinality._1,
            maxCardinality = cardinality._2,
            boundToValueSet = None,
            isValueSetBindingRequired = None,
            referencableProfiles = None,
            constraintDefinitions = None,
            sliceDefinition = None,
            sliceName = None,
            fixedValue = None,
            patternValue = None,
            referringTo = None,
            short = Some(fieldLabel),
            definition = Some(fieldNotes),
            comment = None,
            elements = None
          )
        }
        // check whether it is the record identifier field
        if(!includeRecordIdentifierField && variableName.get.contentEquals(recordIdField)){
          includeRecordIdentifierField = true
        }
      })
      // add record identifier field if it does not exist in the form
      if(!includeRecordIdentifierField){
        definitions = SimpleStructureDefinition(id = recordIdField,
          path = s"$schemaId.${recordIdField}",
          dataTypes = Some(Seq(getDataType(RedCapDataTypes.TEXT))),
          isPrimitive = true, // every field of a REDCap schema is primitive
          isChoiceRoot = false,
          isArray = false, // there is only one record identifier
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
          short = Some("Record Identifier"),
          definition = Some("Unique identifier for individual record within a project"),
          comment = None,
          elements = None
        ) +: definitions
      }

      SchemaDefinition(id = schemaId,
        url = s"$definitionRootUrl/${FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION}/$schemaId",
        version = SchemaDefinition.VERSION_LATEST, // Set the schema version to latest for the REDCap schemas
        `type` = schemaId,
        name = schemaName,
        description = None,
        rootDefinition = None,
        fieldDefinitions = Some(definitions))
    }).toSeq
  }

  /**
   * Returns the minimum and maximum cardinality of a REDCap variable.
   *
   * @param fieldType the data type of REDCap variable. See {@link RedCapDataTypes}
   * @param required  whether the variable is required or not. 'y' is used to indicate that it is required.
   * @return minimum and maximum cardinality as a tuple
   * */
  private def getCardinality(fieldType: String, required: String): (Int, Option[Int]) = {
    val minCardinality = required match {
      case "y" => 1
      case _ => 0
    }
    val maxCardinality = fieldType match {
      case RedCapDataTypes.CHECKBOXES => None
      case RedCapDataTypes.DESCRIPTIVE => Some(0) // descriptive fields are used for decorative purposes, they do not provide any data
      case _ => Some(1)
    }
    (minCardinality, maxCardinality)
  }

  /**
   * Returns the FHIR data type for a REDCap variable whose field and text validation types are given.
   *
   * @param fieldType          the data type of REDCap variable. See {@link RedCapDataTypes}
   * @param textValidationType the text validation type of REDCap variable. See {@link RedCapTextValidationTypes}
   * @return corresponding FHIR data type
   * @throws IllegalArgumentException when invalid data type or text validation types are given
   * */
  private def getDataType(fieldType: String, textValidationType: Option[String] = None): DataTypeWithProfiles = {
    fieldType match {
      case RedCapDataTypes.TEXT | RedCapDataTypes.NOTES =>
        if (textValidationType.nonEmpty) {
          textValidationType.get match {
            case RedCapTextValidationTypes.DATE_DMY => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATE}")))
            case RedCapTextValidationTypes.DATE_MDY => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATE}")))
            case RedCapTextValidationTypes.DATE_YMD => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATE}")))
            case RedCapTextValidationTypes.DATETIME_DMY => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))
            case RedCapTextValidationTypes.DATETIME_MDY => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))
            case RedCapTextValidationTypes.DATETIME_YMD => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))
            case RedCapTextValidationTypes.DATETIME_SECOND_DMY => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))
            case RedCapTextValidationTypes.DATETIME_SECONDS_MDY => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))
            case RedCapTextValidationTypes.DATETIME_SECONDS_YMD => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DATETIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DATETIME}")))
            case RedCapTextValidationTypes.EMAIL => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))
            case RedCapTextValidationTypes.INTEGER => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))
            case RedCapTextValidationTypes.NUMBER => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DECIMAL, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DECIMAL}")))
            case RedCapTextValidationTypes.PHONE => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))
            case RedCapTextValidationTypes.TIME => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.TIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.TIME}")))
            case RedCapTextValidationTypes.ZIP_CODE => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))
            case RedCapTextValidationTypes.POSTAL_CODE_GERMANY => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))
            case RedCapTextValidationTypes.TIME_MM_SS => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.TIME, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.TIME}")))
            case RedCapTextValidationTypes.NUMBER_2DP => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DECIMAL, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DECIMAL}")))
            case "" => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))
            case _ => {
              throw new IllegalArgumentException(s"Invalid text validation type for texts: ${textValidationType.get}")
            }
          }
        } else {
          DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))
        }
      case RedCapDataTypes.RADIO => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.CODE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.CODE}")))
      case RedCapDataTypes.DROPDOWN => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.CODE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.CODE}")))
      case RedCapDataTypes.CHECKBOXES => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.CODE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.CODE}")))
      case RedCapDataTypes.CALC => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.DECIMAL, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.DECIMAL}")))
      case RedCapDataTypes.FILE =>
        if (textValidationType.nonEmpty) {
          textValidationType.get match {
            case RedCapTextValidationTypes.SIGNATURE => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.SIGNATURE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.SIGNATURE}")))
            case "" => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.BASE64BINARY, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.BASE64BINARY}")))
            case _ => {
              throw new IllegalArgumentException(s"Invalid text validation type for files: ${textValidationType.get}")
            }
          }
        } else {
          DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.BASE64BINARY, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.BASE64BINARY}")))
        }
      case RedCapDataTypes.YES_NO => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.BOOLEAN, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.BOOLEAN}")))
      case RedCapDataTypes.TRUE_FALSE => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.BOOLEAN, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.BOOLEAN}")))
      case RedCapDataTypes.DESCRIPTIVE => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.STRING, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.STRING}")))
      case RedCapDataTypes.SLIDER => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.INTEGER, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.INTEGER}")))
      case RedCapDataTypes.SQL => DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.CODE, profiles = Some(Seq(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${FHIR_DATA_TYPES.CODE}")))
      case _ => {
        throw new IllegalArgumentException(s"Invalid data type: $fieldType")
      }
    }
  }
}

/**
 * Keeps the columns of a REDCap data dictionary csv
 * */
object RedCapDataDictionaryColumns {
  val VARIABLE_FIELD_NAME = "Variable / Field Name"
  // When exporting the Data Dictionary file from REDCap, it is encoded with 'UTF-8 with BOM'.
  // The following variable is used to store the name including the BOM for 'Variable / Field Name' column.
  val VARIABLE_FIELD_NAME_WITH_BOM = "\uFEFF\"Variable / Field Name\""
  val FORM_NAME = "Form Name"
  val FIELD_TYPE = "Field Type"
  val FIELD_LABEL = "Field Label"
  val FIELD_NOTES = "Field Notes"
  val REQUIRED_FIELD = "Required Field?"
  val TEXT_VALIDATION_TYPE = "Text Validation Type OR Show Slider Number"
}

/**
 * Available values for {@link RedCapDataDictionaryColumns.FIELD_TYPE}
 * */
object RedCapDataTypes {
  val TEXT = "text" // single-line text box (for text and numbers)
  val NOTES = "notes" // large text box for lots of text
  val RADIO = "radio" // radio buttons for multiple choice options
  val DROPDOWN = "dropdown" // a dropdown menu for multiple choice options
  val CHECKBOXES = "checkbox" // checkboxes for multiple choice options where more than one answer can be chosen
  val CALC = "calc" // perform real-time calculations. Calculations can only result in numbers.
  val FILE = "file" // upload a document
  val YES_NO = "yesno" // radio buttons with yes and no options
  val TRUE_FALSE = "truefalse" // radio buttons with true and false options
  val DESCRIPTIVE = "descriptive" // text displayed with no data entry and optional image/file attachment
  val SLIDER = "slider" // visual analogue scale; coded as 0-100
  val SQL = "sql" // select query statement to populate dropdown choices
}

/**
 * Available values for {@link RedCapDataDictionaryColumns.TEXT_VALIDATION_TYPE}
 * */
object RedCapTextValidationTypes {
  val DATE_DMY = "date_dmy" // 31-12-2008
  val DATE_MDY = "date_mdy" // 12-31-2008
  val DATE_YMD = "date_ymd" // 2008-12-31
  val DATETIME_DMY = "datetime_dmy" // 16-02-2011 17:45
  val DATETIME_MDY = "datetime_mdy" // 02-16-2011 17:45
  val DATETIME_YMD = "datetime_ymd" // 2011-02-16 17:45
  val DATETIME_SECOND_DMY = "datetime_second_dmy" // 16-02-2011 17:45:23
  val DATETIME_SECONDS_MDY = "datetime_seconds_mdy" // 02-16-2011 17:45:23
  val DATETIME_SECONDS_YMD = "datetime_seconds_ymd" // 2011-02-16 17:45:23
  val EMAIL = "email" // john.doe@test
  val INTEGER = "integer" // whole number with no decimal such as 1, 4, -10
  val NUMBER = "number" // (1.3, 22, -6.28) a general number
  val PHONE = "phone" // 615-322-2222)
  val TIME = "time" // (19:30, 04:15) - Military time
  val ZIP_CODE = "zipcode" // (37212, 90210) 5-digit zipcode
  val POSTAL_CODE_GERMANY = "postalcode_germany" // (37212, 90210) 5-digit zipcode
  val TIME_MM_SS = "time_mm_ss" // (MM:SS)
  val SIGNATURE = "signature"
  val NUMBER_2DP = "number_2dp" // number with exactly two decimal places (125.34)
}

