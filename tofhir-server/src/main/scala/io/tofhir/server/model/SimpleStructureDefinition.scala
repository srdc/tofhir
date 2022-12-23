package io.tofhir.server.model

import io.onfhir.api.FHIR_ROOT_URL_FOR_DEFINITIONS

/**
 * Simplified (flat) model for FHIR StructureDefinition
 *
 * @param id
 * @param path
 * @param dataTypes
 * @param isPrimitive
 * @param isChoiceRoot
 * @param isArray
 * @param minCardinality
 * @param maxCardinality
 * @param boundToValueSet
 * @param isValueSetBindingRequired
 * @param referencableProfiles
 * @param constraintDefinitions
 * @param sliceDefinition
 * @param sliceName
 * @param fixedValue
 * @param patternValue
 * @param short
 * @param definition
 * @param comment
 * @param elements
 */
case class SimpleStructureDefinition(id: String,
                                     path: String,
                                     dataTypes: Option[Seq[DataTypeWithProfiles]],
                                     isPrimitive: Boolean,
                                     isChoiceRoot: Boolean,
                                     isArray: Boolean,
                                     minCardinality: Int,
                                     maxCardinality: Option[Int],
                                     boundToValueSet: Option[String],
                                     isValueSetBindingRequired: Option[Boolean],
                                     referencableProfiles: Option[Seq[String]],
                                     constraintDefinitions: Option[Seq[ConstraintDefinition]],
                                     sliceDefinition: Option[SliceDefinition],
                                     sliceName: Option[String],
                                     fixedValue: Option[String],
                                     patternValue: Option[String],
                                     referringTo: Option[String],
                                     short: Option[String],
                                     definition: Option[String],
                                     comment: Option[String],
                                     elements: Option[Seq[SimpleStructureDefinition]]) {

  def withElements(_elements: Seq[SimpleStructureDefinition]): SimpleStructureDefinition = {
    this.copy(elements = Some(_elements))
  }

  def withReferencedContent(newChildElement: SimpleStructureDefinition): SimpleStructureDefinition = {
    this.copy(dataTypes = newChildElement.dataTypes, elements = newChildElement.elements, short = newChildElement.short,
      definition = newChildElement.definition, comment = newChildElement.comment)
  }

  def getProfileUrlForDataType: Option[String] = {
    // If a single dataType exists for the createdElement, use it -->
    //    if profiles are associated with this data type, use the 1st profile
    //    otherwise, use the dataType
    dataTypes match {
      case Some(dts) if dts.length == 1 =>
        dts.head.profiles match {
          case Some(prfls) => Some(prfls.head) // If there are multiple profiles defined for this dataType, consider the 1st one only.
          case None => Some(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${dts.head.dataType}")
        }
      case _ => Option.empty[String]
    }
  }
}

/**
 * A Fhir data type may be associated with a profile (e.g., a code element might be indicated to conform to a CustomCodeableConcept. In this case,
 * the dataType will be CondeableConcept while a single element will exist in profiles list which will be the URL of CustomCodeableConcept.
 *
 * @param dataType Name of the FHIR data type
 * @param profiles URLs of the profiles to which this dataType conforms to
 */
case class DataTypeWithProfiles(dataType: String, profiles: Option[Seq[String]])

object DataTypeWithProfiles {
  def apply(tuple: (String, Seq[String])): DataTypeWithProfiles = {
    DataTypeWithProfiles(tuple._1, if(tuple._2.isEmpty) Option.empty[Seq[String]] else Some(tuple._2))
  }
}

/**
 * Fhir Slicing definition
 *
 * @param discriminators A sequence of Fhir Slicing Discriminators
 * @param ordered        If the elements of slices are ordered
 * @param rule           Rule for slicing (closed | open | openAtEnd)
 */
case class SliceDefinition(discriminators: Seq[SliceDiscriminator], ordered: Boolean, rule: String)
case class SliceDiscriminator(`type`: String, path: String)

/**
 * A FHIR Constraint on an element (Rule --> e.g., + Rule: Must have at least a low or a high or text)
 *
 * @param key       Name of constraint
 * @param desc      Description of constraint
 * @param isWarning If it is only warning
 */
case class ConstraintDefinition(key: String, desc: String, isWarning: Boolean = false)
