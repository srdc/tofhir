package io.tofhir.server.service.fhir

import io.onfhir.api.validation.{ConstraintKeys, ElementRestrictions, ProfileRestrictions}
import io.onfhir.config.BaseFhirConfig
import io.onfhir.validation._
import io.tofhir.common.model.Json4sSupport.formats
import io.tofhir.common.model._
import io.tofhir.server.model.CountingMap
import org.json4s.jackson.Serialization

class SimpleStructureDefinitionService(fhirConfig: BaseFhirConfig) {

  /**
   * Convert ProfileRestrictions into a SchemaDefinition instance.
   *
   * @param profileRestrictions
   * @return
   */
  def convertToSchemaDefinition(schemaId: String, profileRestrictions: ProfileRestrictions): SchemaDefinition = {
    val rootElementDefinition = createRootElement(profileRestrictions.resourceType)
    SchemaDefinition(id = schemaId,
      url = profileRestrictions.url,
      version = profileRestrictions.version.getOrElse(SchemaDefinition.VERSION_LATEST),
      `type` = profileRestrictions.resourceType,
      name = profileRestrictions.resourceName.getOrElse(profileRestrictions.resourceType),
      description = profileRestrictions.resourceDescription,
      rootDefinition = Some(rootElementDefinition),
      fieldDefinitions = Some(simplifyStructureDefinition(profileRestrictions.url, profileRestrictions.version, withResourceTypeInPaths = true)))
  }

  /**
   * Given a URL for a profile, return a sequence of definitions for all elements of the resource type indicated by this profile.
   *
   * @param profileUrl              The URL of the profile to be simplified.
   * @param profileVersion          Version of the profile to be simplified. (profileUrl, version) tuple provides the uniqueness. The same profileUrl can have multiple versions.
   * @param withResourceTypeInPaths If true, the resource type of the given profileUrl will be added to the beginning of all FHIR paths of the inner elements.
   * @return
   */
  def simplifyStructureDefinition(profileUrl: String, profileVersion: Option[String], withResourceTypeInPaths: Boolean = false): Seq[SimpleStructureDefinition] = {

    /**
     * Recursive helper function to create the SimpleStructureDefinition sequence for a given profile, carrying the ElementRestrictions to inner elements.
     *
     * @param profileUrl                    URL of a FHIR profile which can be empty. If empty, only restrictionsFromParentElement will be considered while creating the element definitions.
     * @param profileVersion                Version of the FHIR profile.
     * @param parentPath                    FHIRPath until now. The SimpleStructureDefinitions will be created under the given parentPath.
     * @param restrictionsFromParentElement ElementRestrictions from parent profiles.
     * @param accumulatingTypeUrls          Data types throughout the recursive chain so that recursion can stop if a loop over the data types exists.
     * @return
     */
    def simplifier(profileUrl: Option[String], profileVersion: Option[String], parentPath: Option[String], restrictionsFromParentElement: Seq[(String, ElementRestrictions)], accumulatingTypeUrls: CountingMap[String]): Seq[SimpleStructureDefinition] = {

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
      def createDefinitionWithElements(fieldName: String, sliceName: String, parentPath: Option[String], profileUrlForDataType: Option[String],
                                       restrictionsOnSlicesOfField: Seq[(String, ElementRestrictions)], restrictionsOnChildrenOfField: Seq[(String, ElementRestrictions)],
                                       accumulatingTypeUrls: CountingMap[String], dataTypeWithProfiles: Option[DataTypeWithProfiles] = None): SimpleStructureDefinition = {
        // Partition the restriction into 2: (i) directly on that field (e.g., value[x]:valueQuantity) and (ii) on the children (e.g., value[x]:valueQuantity.system)
        val (restrictionsOnSliceField, restrictionsOnChildrenOfSlices) = restrictionsOnSlicesOfField.partition(_._1 == s"$fieldName:$sliceName")
        val typeRestrictionForThisTypeField = dataTypeWithProfiles.map(dt =>
          ElementRestrictions(path = s"$fieldName:$sliceName", restrictions = Map(ConstraintKeys.DATATYPE -> TypeRestriction(Seq(dt.dataType -> Seq.empty[String]))), sliceName = None, contentReference = None))
        // for the sliced elements, their paths should include the current element. Therefore, pass the field name with parent path to generateSimpleDefinition function
        val createdChoiceTypeElement = generateSimpleDefinition(sliceName, Some(parentPath.map(pp => s"$pp.$fieldName").getOrElse(fieldName)), restrictionsOnSliceField.map(_._2) ++ typeRestrictionForThisTypeField)
        if (createdChoiceTypeElement.isPrimitive) createdChoiceTypeElement
        else {
          val navigatedRestrictionsOnChildrenOfSlices = restrictionsOnChildrenOfSlices
            .filter(res => {
              val searchText = s"$fieldName:$sliceName"
              // startsWith check is not enough when there are some slices starting with the same word such as qtInterval and qtIntervalCorrected
              // therefore, we check the first character following searchText to determine correct restrictions for the slice
              res._1.startsWith(searchText) && res._1.substring(searchText.length).charAt(0).==('.')
            }) // Take the children only for this slice, other slices will also be in restrictionsOnChildrenOfSlices because of our use of partition (above)
            .map(navigateFhirPathFromField(s"$fieldName:$sliceName", _))
          val navigatedRestrictionsOnChildrenOfField = restrictionsOnChildrenOfField
            .map(navigateFhirPathFromField(fieldName, _))
          val navigatedRestrictionsOnChildren =
            (navigatedRestrictionsOnChildrenOfSlices ++ navigatedRestrictionsOnChildrenOfField) // Merge the restrictions on children of the slices and children of the field itself.
              .groupBy(_._1) // Group by using the paths (field names)
              .map { kv => // Iterate over the groups to merge the ElementRestrictions on each field
                kv._1 -> kv._2.map(_._2) // Create the tuple of (fieldName, ElementRestrictions)
                  .reduceLeft((er1, er2) => er1.addNewRestrictions(er2.restrictions)) // Merge the restrictions going from left-to-right by adding the right's restrictions on top of left's.
              }.toSeq
          val dataTypeOfCreatedTypeElement = createdChoiceTypeElement.getProfileUrlForDataType
          val definitionsOfChoiceTypeElementsChildren =
            simplifier(
              profileUrl = if (dataTypeOfCreatedTypeElement.isDefined) dataTypeOfCreatedTypeElement else profileUrlForDataType,
              profileVersion = None,
              parentPath = Some(createdChoiceTypeElement.path),
              restrictionsFromParentElement = navigatedRestrictionsOnChildren,
              accumulatingTypeUrls = accumulatingTypeUrls.apply(profileUrl))
          createdChoiceTypeElement.withElements(definitionsOfChoiceTypeElementsChildren)
        }
      }

      // Start of the simplifier method
      if (profileUrl.isDefined &&
        // Stop the recursion if the profile is already visited twice (to avoid infinite recursion)
        (accumulatingTypeUrls.get(profileUrl.get)>= 2 ||
        // The following is a hack to decrease the size of the SimpleStructureDefinition because Identifier type occurs in many places in the FHIR spec.
        // If more examples like this are found, either we should find a better way to stop the recursion or we should add them here
        (profileUrl.get == "http://hl7.org/fhir/StructureDefinition/Identifier") && accumulatingTypeUrls.get(profileUrl.get) >= 1)) {
        // Stop the recursion here because we are entering into a recursive type chain (e.g., Identifier -> Reference -> Identifier)
        Seq.empty[SimpleStructureDefinition]
      } else {
        val profileRestrictionsSeq: Seq[ProfileRestrictions] = profileUrl.map(url => fhirConfig.findProfileChain(url, profileVersion)).getOrElse(Seq.empty[ProfileRestrictions])
        val elementRestrictionsFromProfile = profileRestrictionsSeq
          .flatMap { pr =>
            // Make a list of all ElementRestrictions (respect their order)
            // But, filter out extension, modifierExtension and id fields if they come from Element and BackboneElement profiles. Otherwise, the SimpleStructureDefinition becomes huge!
            if (pr.url.endsWith("Element") || pr.url.endsWith("BackboneElement"))
              pr.elementRestrictions.filterNot(er => er._1 == "extension" || er._1 == "modifierExtension" || er._1 == "id")
            // remove the extensions coming from DomainResource. SimpleStructureDefinition will include "extension" element
            // iff the profile has some slices for it.
            else if (pr.url.endsWith("DomainResource"))
              pr.elementRestrictions.filterNot(er => er._1 == "extension"|| er._1 == "modifierExtension")
            else
              pr.elementRestrictions
          }
        val allRestrictions = restrictionsFromParentElement ++ elementRestrictionsFromProfile

        import io.tofhir.server.util.GroupByOrdered._

        val groupedFieldRestrictions = allRestrictions
          .filterNot(r => r._1.split('.').head.contains(":")) // Eliminate the slice definitions such as coding:aic or value[x]:valueQuantity, but not code.coding:aic
          .groupByASequenceOrdered(r => r._1.split('.').head) // Group by immediate field name (e.g., group {code, code.coding.system, code.coding.value} together)

        groupedFieldRestrictions.flatMap { // flatMap to get rid of the None objects in the sequence
          case (fieldName, restrictionsOnFieldAndItsChildren) =>
            val (restrictionsOnField, restrictionsOnChildren) = restrictionsOnFieldAndItsChildren.partition(_._1 == fieldName)

            // Create the SimpleStructureDefinition for this fieldName
            val createdElementDefinition = generateSimpleDefinition(fieldName, parentPath, restrictionsOnField.map(_._2))

            if(createdElementDefinition.maxCardinality.contains(0)) {
              Option.empty[SimpleStructureDefinition] // Do not put an element into our result if it is removed (max-cardinality = 0) in the profile definitions.
            }
            else if (createdElementDefinition.isPrimitive && !createdElementDefinition.isChoiceRoot) {
              // For choice fields (e.g., value[x]), if it has a single simple type (e.g., boolean), then we count is as primitive.
              // That's why the extra check on whether it is choiceRoot or not.
              Some(createdElementDefinition)
            } else {
              val restrictionsOnSlicesOfField = allRestrictions.filter(t => t._1.startsWith(s"$fieldName:"))
              if (createdElementDefinition.isChoiceRoot) {
                // Add the complex types of the choice as restrictions under this field so that they are created as elements
                val definitionsOfChoiceTypes: Seq[SimpleStructureDefinition] = createdElementDefinition.dataTypes match {
                  case Some(typesWithProfiles) =>
                    if (restrictionsOnSlicesOfField.isEmpty) {
                      // If there are not restrictions for any of the data types of the choice, then do not populate them.
                      // The client can make further requests to retrieve simplified definitions of them.
                      Seq.empty[SimpleStructureDefinition]
                    } else {
                      typesWithProfiles.map { dt =>
                        val choiceTypeFieldName = s"${fieldName.replace("[x]", "")}${dt.dataType.capitalize}" // Create the field name such as valueQuantity, valueBoolean etc.
                        createDefinitionWithElements(fieldName, choiceTypeFieldName, parentPath, None, restrictionsOnSlicesOfField, Seq.empty, accumulatingTypeUrls, Some(dt))
                      }
                    }
                  case None => throw new IllegalArgumentException("A choice root cannot exist without any data types!!")
                }
                Some(createdElementDefinition.withElements(definitionsOfChoiceTypes))
              } else if (createdElementDefinition.sliceDefinition.isDefined) {
                val sliceNames = restrictionsOnSlicesOfField.collect {
                  // consider only the direct slices of field
                  case t if t._2.sliceName.isDefined && t._1.contentEquals(s"$fieldName:${t._2.sliceName.get}") => t._2.sliceName.get
                }.distinct // Read the following explanation
                // Same slice (using the same slice name) can be extended/restricted within the profile definitions tree.
                // We take only one of these slices at this point, but we send the whole restrictions to createDefinitionWithElements function
                // in which we partition those restrictions w.r.t the slice name which allows us to take all restrictions on the slice.
                // This means we process the deepest restrictions on the slice and apply in the order of the profile tree as we handle in generateSimpleDefinition function.
                val definitionsOfSlices: Seq[SimpleStructureDefinition] = sliceNames.map { sliceFieldName =>
                  createDefinitionWithElements(fieldName, sliceFieldName, parentPath, createdElementDefinition.getProfileUrlForDataType, restrictionsOnSlicesOfField, restrictionsOnChildren, accumulatingTypeUrls)
                }
                val createdNoSliceElement = generateSimpleDefinition("No Slice", parentPath, Seq.empty[ElementRestrictions])
                val navigatedRestrictionsOnChildren = restrictionsOnChildren.map(navigateFhirPathFromField(fieldName, _))
                val createdNoSliceElementWithChildren = createdNoSliceElement
                  .withElements(simplifier(createdElementDefinition.getProfileUrlForDataType, None, Some(createdNoSliceElement.path), navigatedRestrictionsOnChildren, accumulatingTypeUrls.apply(profileUrl)))
                Some(createdElementDefinition.withElements(createdNoSliceElementWithChildren +: definitionsOfSlices))
              } else {
                val navigatedRestrictionsOnChildren = restrictionsOnChildren.map(navigateFhirPathFromField(fieldName, _))
                val definitionsOfChildren =
                  simplifier(profileUrl = createdElementDefinition.getProfileUrlForDataType,
                    profileVersion = None,
                    parentPath = Some(createdElementDefinition.path),
                    restrictionsFromParentElement = navigatedRestrictionsOnChildren,
                    accumulatingTypeUrls = accumulatingTypeUrls.apply(profileUrl))
                Some(createdElementDefinition.withElements(definitionsOfChildren))
              }
            }
        }
      }
    }

    // Start of the simplifyStructureDefinition method
    val simplifiedElementsOfProfile = simplifier(
      profileUrl = Some(profileUrl),
      profileVersion = profileVersion,
      parentPath = if (withResourceTypeInPaths) fhirConfig.findResourceType(profileUrl) else Option.empty[String],
      restrictionsFromParentElement = Seq.empty[(String, ElementRestrictions)],
      accumulatingTypeUrls = CountingMap.empty[String])

    // Handle the content references to populate them.
    populateContentReferences(simplifiedElementsOfProfile, simplifiedElementsOfProfile)
  }

  /**
   * Recursively iterate over the given sequence of SimpleStructureDefinitions and resolve the definitions with a
   * reference to another definition.
   *
   * @param rootElements
   * @param children
   * @return The updated sequence of definitions
   */
  private def populateContentReferences(rootElements: Seq[SimpleStructureDefinition], children: Seq[SimpleStructureDefinition]): Seq[SimpleStructureDefinition] = {

    /**
     * Find the element at path.
     *
     * @param path     FHIRPath of the element to be fetched.
     * @param elements Sequence of SimpleStructureDefinition elemenets
     * @return The element at path or None if cannot be found.
     */
    def findElementDefinition(path: List[String], elements: Seq[SimpleStructureDefinition]): Option[SimpleStructureDefinition] = {
      path match {
        case head :: Nil =>
          elements.find(_.id == head)
        case head :: tail =>
          elements.find(_.id == head) flatMap { matchingElement =>
            if (matchingElement.elements.isEmpty) {
              throw new IllegalArgumentException(s"Reached at the end of elements! Cannot go further for the remaining path:${tail.mkString(".")}")
            } else {
              findElementDefinition(tail, matchingElement.elements.get)
            }
          }
        case Nil =>
          throw new IllegalArgumentException("A path must exist to find an element definition within Seq[SimpleStructureDefinition]")
      }
    }

    children.map { elementDefinition =>
      if (elementDefinition.referringTo.isEmpty) {
        elementDefinition.elements match {
          case Some(elementCh) => elementDefinition.withElements(populateContentReferences(rootElements, elementCh))
          case None => elementDefinition
        }
      } else {
        val referencePath = elementDefinition.referringTo.get.split('.').toList
        val referencedElement = findElementDefinition(referencePath, rootElements)
        referencedElement match {
          case Some(el) => elementDefinition.withReferencedContent(el)
          case None => throw new IllegalStateException(s"Reference path of the element does not refer to any definition. Element:${elementDefinition.id} -- referencePath:${elementDefinition.referringTo.get}")
        }
      }
    }

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
    var contentReference: Option[String] = None
    var shortDescription: Option[String] = None
    var definition: Option[String] = None
    var comment: Option[String] = None

    restrictionsOnField.foreach { elementRestrictions =>
      shortDescription = elementRestrictions.metadata.flatMap(md => md.short)
      definition = elementRestrictions.metadata.flatMap(md => md.definition)
      comment = elementRestrictions.metadata.flatMap(md => md.comment)

      if (elementRestrictions.contentReference.isDefined) {
        if (contentReference.isEmpty) {
          contentReference = elementRestrictions.contentReference
        }
      }

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
          if (fixedOrPatternRestriction.isFixed) {
            if (fixedValue.isEmpty) {
              fixedValue = Some(Serialization.write(fixedOrPatternRestriction.fixedValue))
            }
          } else {
            if (patternValue.isEmpty) {
              patternValue = Some(Serialization.write(fixedOrPatternRestriction.fixedValue))
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
      referringTo = contentReference,
      short = shortDescription,
      definition = definition,
      comment = comment,
      elements = None)
  }

  /**
   * Helper function to create the root element (1st element of the Element definitions which is dropped by IFhirFoundationParser.
   *
   * @param resourceType
   * @return
   */
  private def createRootElement(resourceType: String): SimpleStructureDefinition = {
    SimpleStructureDefinition(
      id = resourceType,
      path = resourceType,
      dataTypes = Some(Seq(DataTypeWithProfiles("Element", None))),
      isPrimitive = false,
      isChoiceRoot = false,
      isArray = false,
      minCardinality = 0, maxCardinality = None,
      boundToValueSet = None,
      isValueSetBindingRequired = None,
      referencableProfiles = None,
      constraintDefinitions = None,
      sliceDefinition = None,
      sliceName = None,
      fixedValue = None, patternValue = None,
      referringTo = None,
      short = None,
      definition = None,
      comment = None,
      elements = None)
  }
}
