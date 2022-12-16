package io.tofhir.server.service

import io.onfhir.api.validation.{ConstraintKeys, ElementRestrictions, ProfileRestrictions}
import io.onfhir.config.BaseFhirConfig
import io.onfhir.validation._
import io.tofhir.server.model._

class SimpleStructureDefinitionService(fhirConfig: BaseFhirConfig) {

  /**
   * Given a URL for a profile, return a sequence of definitions for all elements of the resource type indicated by this profile.
   *
   * @param profileUrl
   * @return
   */
  def simplifyStructureDefinition(profileUrl: String): Seq[SimpleStructureDefinition] = {

    /**
     * Recursive helper function to create the SimpleStructureDefinition sequence for a given profile, carrying the ElementRestrictions to inner elements.
     *
     * @param profileUrl                    URL of a FHIR profile which can be empty. If empty, only restrictionsFromParentElement will be considered while creating the element definitions.
     * @param parentPath                    FHIRPath until now. The SimpleStructureDefinitions will be created under the given parentPath.
     * @param restrictionsFromParentElement ElementRestrictions from parent profiles.
     * @param accumulatingTypeUrls          Data types throughout the recursive chain so that recursion can stop if a loop over the data types exists.
     * @return
     */
    def simplifier(profileUrl: Option[String], parentPath: Option[String], restrictionsFromParentElement: Seq[(String, ElementRestrictions)], accumulatingTypeUrls: Set[String]): Seq[SimpleStructureDefinition] = {

      /**
       * Helper function to create a single SimpleStructureDefinition for a slice of a field (e.g., valueQuantity under value[x])
       * together with inner elements (by calling the simplifier recursively)
       *
       * @param fieldName                   The name of the parent field (e.g., coding)
       * @param sliceName                   The name of the slice under the parent field (e.g., aicNonMotorSymptom)
       * @param parentPath                  FHIRPath under which the definition will be created.
       * @param profileUrlForDataType       For non-choice slices, URL of the profile for the data type (if exists)
       * @param restrictionsOnSlicesOfField ElementRestrictions on the slice
       * @param accumulatingTypeUrls        Accumulating data types to break the loop in the recursion
       * @param dataTypeWithProfiles        For choice field slices, the data type for this slice
       * @return
       */
      def createDefinitionWithElements(fieldName: String, sliceName: String, parentPath: Option[String], profileUrlForDataType: Option[String], restrictionsOnSlicesOfField: Seq[(String, ElementRestrictions)],
                                       accumulatingTypeUrls: Set[String], dataTypeWithProfiles: Option[DataTypeWithProfiles] = None): SimpleStructureDefinition = {
        // Partition the restriction into 2: (i) directly on that field (e.g., value[x]:valueQuantity) and (ii) on the children (e.g., value[x]:valueQuantity.system)
        val (restrictionsOnSliceField, restrictionsOnChildrenOfSlice) = restrictionsOnSlicesOfField.partition(_._1 == s"$fieldName:$sliceName")
        val typeRestrictionForThisTypeField = dataTypeWithProfiles.map(dt =>
          ElementRestrictions(path = s"$fieldName:$sliceName", restrictions = Map(ConstraintKeys.DATATYPE -> TypeRestriction(Seq(dt.dataType -> Seq.empty[String]))), sliceName = None, contentReference = None))
        val createdChoiceTypeElement = generateSimpleDefinition(sliceName, parentPath, restrictionsOnSliceField.map(_._2) ++ typeRestrictionForThisTypeField)
        if (createdChoiceTypeElement.isPrimitive) createdChoiceTypeElement
        else {
          val navigatedRestrictionsOnChildren = restrictionsOnChildrenOfSlice.map(navigateFhirPathFromField(s"$fieldName:$sliceName", _))
          val dataTypeOfCreatedTypeElement = createdChoiceTypeElement.getProfileUrlForDataType
          val definitionsOfChoiceTypeElementsChildren =
            simplifier(
              profileUrl = if (dataTypeOfCreatedTypeElement.isDefined) dataTypeOfCreatedTypeElement else profileUrlForDataType,
              parentPath = Some(createdChoiceTypeElement.path),
              restrictionsFromParentElement = navigatedRestrictionsOnChildren,
              accumulatingTypeUrls = accumulatingTypeUrls ++ profileUrl)
          createdChoiceTypeElement.withElements(definitionsOfChoiceTypeElementsChildren)
        }
      }

      // Start of the simplifier method
      if (profileUrl.isDefined && accumulatingTypeUrls.contains(profileUrl.get)) {
        // Stop the recursion here because we are entering into a recursive type chain (e.g., Identifier -> Reference -> Identifier)
        Seq.empty[SimpleStructureDefinition]
      } else {
        val profileRestrictionsSeq: Seq[ProfileRestrictions] = if (profileUrl.isDefined) fhirConfig.findProfileChain(profileUrl.get) else Seq.empty[ProfileRestrictions]
        val elementRestrictionsFromProfile = profileRestrictionsSeq
          .flatMap { pr =>
            // Make a list of all ElementRestrictions (respect their order)
            // But, filter out extension and modifierExtension fields if they come from Element and BackboneElement profiles. Otherwise the SimpleStructureDefinition becomes huge!
            if(pr.url.endsWith("Element") || pr.url.endsWith("BackboneElement"))
              pr.elementRestrictions.filterNot(er => er._1 == "extension" || er._1 == "modifierExtension")
            else
              pr.elementRestrictions
          }
        val allRestrictions = restrictionsFromParentElement ++ elementRestrictionsFromProfile

        val groupedFieldRestrictions = allRestrictions
          .filterNot(r => r._1.split('.').head.contains(":")) // Eliminate the slice definitions such as coding:aic or value[x]:valueQuantity, but not code.coding:aic
          .groupBy(r => r._1.split('.').head) // Group by immediate field name (e.g., group {code, code.coding.system, code.coding.value} together)

        groupedFieldRestrictions.map {
            case (fieldName, restrictionsOnFieldAndItsChildren) =>
              val (restrictionsOnField, restrictionsOnChildren) = restrictionsOnFieldAndItsChildren.partition(_._1 == fieldName)

              // Create the SimpleStructureDefinition for this fieldName
              val createdElementDefinition = generateSimpleDefinition(fieldName, parentPath, restrictionsOnField.map(_._2))

              if (createdElementDefinition.isPrimitive && !createdElementDefinition.isChoiceRoot) {
                // For choice fields (e.g., value[x]), if it has a single simple type (e.g., boolean), then we count is as primitive.
                // That's why the extra check on whether it is choiceRoot or not.
                createdElementDefinition
              } else {
                val restrictionsOnSlicesOfField = allRestrictions.filter(t => t._1.startsWith(s"$fieldName:"))
                if (createdElementDefinition.isChoiceRoot) {
                  // Add the complex types of the choice as restrictions under this field so that they are created as elements
                  val definitionsOfChoiceTypes: Seq[SimpleStructureDefinition] = createdElementDefinition.dataTypes match {
                    case Some(typesWithProfiles) if typesWithProfiles.length >= 5 =>
                      Seq.empty[SimpleStructureDefinition]
                    case Some(typesWithProfiles) =>
                      typesWithProfiles.map { dt =>
                        val choiceTypeFieldName = s"${fieldName.replace("[x]", "")}${dt.dataType.capitalize}" // Create the field name such as valueQuantity, valueBoolean etc.
                        createDefinitionWithElements(fieldName, choiceTypeFieldName, parentPath, None, restrictionsOnSlicesOfField, accumulatingTypeUrls, Some(dt))
                      }
                    case None => throw new IllegalArgumentException("A choice root cannot exist without any data types!!")
                  }
                  createdElementDefinition.withElements(definitionsOfChoiceTypes)
                } else if (createdElementDefinition.sliceDefinition.isDefined) {
                  val sliceNames = restrictionsOnSlicesOfField.collect {
                    case t if t._2.sliceName.isDefined => t._2.sliceName.get
                  }
                  val definitionsOfSlices: Seq[SimpleStructureDefinition] = sliceNames.map { sliceFieldName =>
                    createDefinitionWithElements(fieldName, sliceFieldName, parentPath, createdElementDefinition.getProfileUrlForDataType, restrictionsOnSlicesOfField, accumulatingTypeUrls)
                  }
                  val createdSliceElement = generateSimpleDefinition("No Slice", parentPath, Seq.empty[ElementRestrictions])
                    .withElements(simplifier(createdElementDefinition.getProfileUrlForDataType, parentPath, Seq.empty, accumulatingTypeUrls ++ profileUrl))
                  createdElementDefinition.withElements(createdSliceElement +: definitionsOfSlices)
                } else {
                  val navigatedRestrictionsOnChildren = restrictionsOnChildren.map(navigateFhirPathFromField(fieldName, _))
                  val definitionsOfChildren =
                    simplifier(profileUrl = createdElementDefinition.getProfileUrlForDataType,
                      parentPath = Some(createdElementDefinition.path),
                      restrictionsFromParentElement = navigatedRestrictionsOnChildren,
                      accumulatingTypeUrls = accumulatingTypeUrls ++ profileUrl)
                  createdElementDefinition.withElements(definitionsOfChildren)
                }
              }
          }.toSeq
      }
    }

    // Start of the simplifyStructureDefinition method
    simplifier(profileUrl = Some(profileUrl),
      parentPath = Option.empty[String],
      restrictionsFromParentElement = Seq.empty[(String, ElementRestrictions)],
      accumulatingTypeUrls = Set.empty[String])
  }

  /**
   * Given the (path, ElementRestrictions) tuple on a field, navigate 1-step on the FHIR path.
   *
   * @param fieldName
   * @param restriction
   * @return
   */
  private def navigateFhirPathFromField(fieldName: String, restriction: (String, ElementRestrictions)): (String, ElementRestrictions) = {
    val path = restriction._1
    val arr = path.split('.')
    if (arr.head != fieldName) {
      throw new IllegalStateException(s"This path $path does not belong to the element $fieldName")
    }
    val newPath = arr.takeRight(arr.length - 1).mkString(".")
    if (newPath.isEmpty) {
      throw new IllegalStateException(s"There is a child path with $path in this field:$fieldName which navigates to the field itself, not to any child element!")
    }
    newPath -> restriction._2
  }

  /**
   * Given the field name, FHIRPath until this field (if exists), and ElementRestrictions directly on this field, create a SimpleStructureDefinition representing this field.
   *
   * @param fieldName
   * @param parentPath
   * @param restrictionsOnField
   * @return
   */
  private def generateSimpleDefinition(fieldName: String, parentPath: Option[String], restrictionsOnField: Seq[ElementRestrictions]): SimpleStructureDefinition = {
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
    val fhirPath: String = parentPath match {
      case None => fieldName
      case Some(path) => s"$path.$fieldName"
    }
    var shortDescription: Option[String] = None
    var definition: Option[String] = None
    var comment: Option[String] = None

    restrictionsOnField.foreach { elementRestrictions =>
      shortDescription = elementRestrictions.metadata.flatMap(md => md.short)
      definition = elementRestrictions.metadata.flatMap(md => md.definition)
      comment = elementRestrictions.metadata.flatMap(md => md.comment)

      if (elementRestrictions.slicing.isDefined) {
        // This is a slice definition for this fieldName, process the most upper-level one only.
        if (sliceDefinition.isEmpty) {
          val fhirSlicing = elementRestrictions.slicing.get
          sliceDefinition = Some(SliceDefinition(
            fhirSlicing.discriminators.map(d => SliceDiscriminator(d._1, d._2)),
            fhirSlicing.ordered,
            fhirSlicing.rule))
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

}
