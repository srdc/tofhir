package io.tofhir.server.model

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
                                     short: Option[String],
                                     definition: Option[String],
                                     comment: Option[String],
                                     elements: Option[Seq[SimpleStructureDefinition]]) {

  def withElements(_elements: Seq[SimpleStructureDefinition]): SimpleStructureDefinition = {
    this.copy(elements = Some(_elements))
  }

}

/**
 * A Fhir data type may be associated with a profile (e.g., a code element might be indicated to conform to a CustomCodeableConcept. In this case,
 * the dataType will be CondeableConcept while a single element will exist in profiles list which will be the URL of CustomCodeableConcept.
 * @param dataType
 * @param profiles
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
