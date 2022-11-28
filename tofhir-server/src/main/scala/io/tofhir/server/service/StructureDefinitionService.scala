package io.tofhir.server.service

import io.onfhir.api.FHIR_ROOT_URL_FOR_DEFINITIONS
import io.onfhir.api.validation.{ElementRestrictions, ProfileRestrictions}
import io.onfhir.validation._
import io.tofhir.server.model._

object StructureDefinitionService {

  val ignoreSet: Set[String] = Set("DomainResource", "BackboneElement", "Element", "Extension")

  def simplifyStructureDefinition(profileUrl: String): Seq[SimpleStructureDefinition] = {

    def simplifier(profileUrl: Option[String], restrictionsFromParentElement: Seq[(String, ElementRestrictions)], accumulatingTypeUrls: Set[String], typeOfElement: Option[DataTypeWithProfiles] = None): Seq[SimpleStructureDefinition] = {
      if (profileUrl.isDefined && accumulatingTypeUrls.contains(profileUrl.get)) {
        // Stop the recursion here because we are entering into a recursive type chain (e.g., Identifier -> Reference -> Identifier)
        Seq.empty[SimpleStructureDefinition]
      } else {
        val profileRestrictionsSeq: Seq[ProfileRestrictions] = /*if (profileUrl.isDefined) fhirConfig.findProfileChain(profileUrl.get) else*/ Seq.empty[ProfileRestrictions]
        val elementRestrictionsFromProfile = profileRestrictionsSeq.flatMap(_.elementRestrictions) // Make a list of all ElementRestrictions (respect their order)
        val allRestrictionsWithoutSlicesComingFromTheProfile = restrictionsFromParentElement ++ elementRestrictionsFromProfile.filterNot(r => r._1.contains(":"))
        val allRestrictions = restrictionsFromParentElement ++ elementRestrictionsFromProfile

        val result = allRestrictionsWithoutSlicesComingFromTheProfile
          .groupBy(r => r._1.split(Array('.')).head) // Group by immediate field name (e.g., group {code, code.coding.system, code.coding.value} together) (e.g., {value[x]:valueQuantity, value[x]:valueQuantity.code} together)
          .map {
            case (fieldName, restrictionsOnFieldAndItsChildren) =>
              val (restrictionsOnField, restrictionsOnChildren) = restrictionsOnFieldAndItsChildren.partition(_._1 == fieldName)

              // Create the SimpleStructureDefinition for this fieldName
              val createdElementDefinition = generateSimpleDefinition(fieldName, restrictionsOnField.map(_._2))

              if (createdElementDefinition.isPrimitive ||
                (createdElementDefinition.dataTypes.isDefined && ignoreSet.contains(createdElementDefinition.dataTypes.get.head.dataType))) {
                createdElementDefinition
              } else {

                // If a single dataType exists for the createdElement, use it -->
                //    if profiles are associated with this data type, use the 1st profile
                //    otherwise, use the dataType
                val profileUrlForDataType: Option[String] = createdElementDefinition.dataTypes match {
                  case Some(dts) if dts.length == 1 =>
                    dts.head.profiles match {
                      case Some(prfls) => Some(prfls.head) // If there are multiple profiles defined for this dataType, consider the 1st one only.
                      case None => Some(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/${dts.head.dataType}")
                    }
                  case _ => Option.empty[String]
                }

                var navigatedRestrictionsOnChildren = restrictionsOnChildren.map(navigateFhirPathFromField(fieldName, _))
                if (createdElementDefinition.sliceDefinition.isDefined) {
                  navigatedRestrictionsOnChildren = navigatedRestrictionsOnChildren ++ allRestrictions.filter(t => t._1.startsWith(s"$fieldName:"))
                }
                val definitionsOfChildren = simplifier(profileUrlForDataType, navigatedRestrictionsOnChildren, accumulatingTypeUrls ++ profileUrl)
                createdElementDefinition.withElements(definitionsOfChildren)
              }
          }.toSeq

        result
      }
    }

    simplifier(Some(profileUrl), Seq.empty[(String, ElementRestrictions)], Set.empty[String])
  }

  private def navigateFhirPathFromField(fieldName: String, restriction: (String, ElementRestrictions)): (String, ElementRestrictions) = {
    val path = restriction._1
    val arr = path.split(Array('.'))
    if (arr.head != fieldName) {
      throw new IllegalStateException(s"This path:$path does not belong to the element:$fieldName")
    }
    val newPath = arr.takeRight(arr.length - 1).mkString(".")
    if (newPath.isEmpty) {
      throw new IllegalStateException(s"There is a child path with $path in this field:$fieldName which navigates to the field itself, not to any child element!")
    }
    newPath -> restriction._2
  }

  def generateSimpleDefinition(fieldName: String, restrictionsOnField: Seq[ElementRestrictions]): SimpleStructureDefinition = {
    var dataTypes: Option[Seq[DataTypeWithProfiles]] = None
    var isArray: Boolean = false
    val isChoiceRoot: Boolean = fieldName.endsWith("[x]")
    var sliceDefinition: Option[SliceDefinition] = None
    var sliceName: Option[String] = None
    var minCardinality: Option[Int] = None
    var maxCardinality: Option[Int] = None
    var valueSetUrl: Option[String] = None
    var isValueSetBindingRequired: Option[Boolean] = None
    var referencableProfiles: Option[Seq[String]] = None
    var constraintDefinitions: Seq[ConstraintDefinition] = Seq.empty
    var fixedValue: Option[String] = None
    var patternValue: Option[String] = None
    var fhirPath: String = ""
    var shortDescription: Option[String] = None
    var definition: Option[String] = None
    var comment: Option[String] = None

    restrictionsOnField.foreach { elementRestrictions =>
      if (elementRestrictions.path.length > fhirPath.length) {
        fhirPath = elementRestrictions.path
      }

      shortDescription = elementRestrictions.metadata.flatMap(md => md.short)
      definition = elementRestrictions.metadata.flatMap(md => md.definition)
      comment = elementRestrictions.metadata.flatMap(md => md.comment)

      if (elementRestrictions.slicing.isDefined) {
        // This is a slice definition for this fieldName
        if (sliceDefinition.isEmpty) {
          val fhirSlicing = elementRestrictions.slicing.get
          sliceDefinition = Some(SliceDefinition(
            fhirSlicing.discriminators.map(d => SliceDiscriminator(d._1, d._2)),
            fhirSlicing.ordered,
            fhirSlicing.rule))
        } else {
          throw new IllegalArgumentException(s"There are multiple slice definitions for this $fieldName. I only expect a single slice definition for a field!")
        }
      }

      if (elementRestrictions.sliceName.isDefined) {
        if (sliceName.isEmpty) {
          sliceName = Some(elementRestrictions.sliceName.get)
        } else {
          throw new IllegalArgumentException(s"There are multiple slice names for this $fieldName. I only expect a single slice name for a field!")
        }
      }

      // Iterate over the restriction to process the constraints by respecting the order given the type of the restriction.
      elementRestrictions.restrictions.values.toSeq.map {
        case typeRestriction: TypeRestriction =>
          if (dataTypes.isEmpty) {
            dataTypes = Some(typeRestriction.dataTypesAndProfiles.map(DataTypeWithProfiles(_)))
            if (!isChoiceRoot) {
              if (typeRestriction.dataTypesAndProfiles.length > 1) {
                throw new NotImplementedError(s"Although the field is not a choice of data types, there are more than one data types in the TypeRestriction of this field:$fieldName. " +
                  s"List of type restrictions:${typeRestriction.dataTypesAndProfiles.map(tr => tr._1 -> tr._2.mkString)}")
              }
            }
          }
        case arrayRestriction: ArrayRestriction =>
          isArray = arrayRestriction.isArray // Assign the last one after the traversal of all restrictions within the ElementRestrictions
        case minRestriction: CardinalityMinRestriction =>
          if (minCardinality.isEmpty) {
            minCardinality = Some(minRestriction.n)
          }
        case maxRestriction: CardinalityMaxRestriction =>
          if (maxCardinality.isEmpty) {
            maxCardinality = Some(maxRestriction.n)
          }
        case codeBindingRestriction: CodeBindingRestriction =>
          if (valueSetUrl.isEmpty) {
            valueSetUrl = Some(codeBindingRestriction.valueSetUrl)
            isValueSetBindingRequired = Some(codeBindingRestriction.isRequired)
          }
        case referenceRestrictions: ReferenceRestrictions =>
          if (referencableProfiles.isEmpty) {
            referencableProfiles = Some(referenceRestrictions.targetProfiles)
          }
        case constraintsRestriction: ConstraintsRestriction =>
          // TODO: Shall we accumulate these constraints or shall we only get the last one?
          constraintDefinitions = constraintDefinitions ++ constraintsRestriction.fhirConstraints.map(fc => ConstraintDefinition(fc.key, fc.desc, fc.isWarning))
        case fixedOrPatternRestriction: FixedOrPatternRestriction =>
          import io.onfhir.util.JsonFormatter._
          if (fixedOrPatternRestriction.isFixed) {
            if (fixedValue.isEmpty) {
              fixedValue = Some(fixedOrPatternRestriction.fixedValue.toJson)
            }
          } else {
            if (patternValue.isEmpty) {
              patternValue = Some(fixedOrPatternRestriction.fixedValue.toJson)
            }
          }
        case unk =>
          throw new IllegalArgumentException(s"Unknown FhirRestriction! ${unk.toString}")
      }
    }

    if (sliceDefinition.isDefined && sliceName.isDefined) {
      throw new IllegalStateException(s"A field cannot be a slice definition and a part of a slice (with slice name) at the same time!!! FieldName:$fieldName")
    }

    val isPrimitive = dataTypes.isDefined && dataTypes.get.length == 1 && Character.isLowerCase(dataTypes.get.head.dataType.head)

    // Integrity check for the FHIR paths of the ElementRestrictions
    val integrityPath = restrictionsOnField
      .map(r => r.path)
      .sortWith((s1, s2) => s1.length < s2.length)
      .foldLeft("")((f, p) => if (p.endsWith(f)) p else "-1")
    if (integrityPath != fhirPath) {
      throw new IllegalArgumentException(s"Given FHIR paths for field:$fieldName in its ElementRestrictions are not all pointing to this same field.")
    }

    SimpleStructureDefinition(id = fieldName,
      path = fhirPath,
      dataTypes = dataTypes,
      isPrimitive = isPrimitive,
      isChoiceRoot = isChoiceRoot,
      isArray = isArray,
      minCardinality = minCardinality.getOrElse(0),
      maxCardinality = maxCardinality,
      boundToValueSet = valueSetUrl,
      isValueSetBindingRequired = isValueSetBindingRequired,
      referencableProfiles = referencableProfiles,
      constraintDefinitions = if (constraintDefinitions.isEmpty) None else Some(constraintDefinitions),
      sliceDefinition = sliceDefinition,
      sliceName = sliceName,
      fixedValue = fixedValue,
      patternValue = patternValue,
      short = shortDescription,
      definition = definition,
      comment = comment,
      elements = None)
  }

  def main(args: Array[String]): Unit = {

//    //Initialize onfhir for R4
//    val onfhir = Onfhir.apply(new FhirR4Configurator())
//    val fhirConfig = FhirConfigurationManager.fhirConfig

    //        StructureDefinitionService.simplifyStructureDefinition(s"$FHIR_ROOT_URL_FOR_DEFINITIONS/StructureDefinition/Identifier")
    val groundedModel = StructureDefinitionService.simplifyStructureDefinition("https://aiccelerate.eu/fhir/StructureDefinition/AIC-ParkinsonNonMotorSymptomAssessment")

    import io.onfhir.util.JsonFormatter._
    import org.json4s.jackson.Serialization
    println(Serialization.write(groundedModel))
  }

}
