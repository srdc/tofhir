package io.tofhir.engine.mapping

import io.onfhir.api.util.FHIRUtil
import io.onfhir.path._
import io.onfhir.path.annotation.FhirPathFunction
import io.onfhir.path.grammar.FhirPathExprParser.ExpressionContext
import io.tofhir.engine.model.{ConceptMapContext, FhirMappingContext, UnitConversionContext}
import io.tofhir.engine.util.FhirMappingUtility
import org.json4s.{JObject, JString}


/**
 * Function library for FHIR Path expressions that provide mapping utility functions
 *
 * @param context        FHIR Path context
 * @param current        Current result to apply the function on
 * @param mappingContext Specific mapping context
 */
class FhirPathMappingFunctions(context: FhirPathEnvironment, current: Seq[FhirPathResult], mappingContext: Map[String, FhirMappingContext])
  extends AbstractFhirPathFunctionLibrary with Serializable {

  /**
   * Get hash of a string to generate a
   * @param inputExpr
   * @return
   */
  @FhirPathFunction(
    documentation = "Creates an id using the hash of given string.Resource name should be quoted and id should be string Ex: mpp:getHashedId('Encounter', id.toString())",
    insertText = "mpp:getHashedId(<resourceName>, <id>)",
    detail = "mpp",
    label = "mpp:getHashedId",
    kind = "Function",
    returnType = Seq("string"),
    inputType = Seq()
  )
  def getHashedId(resourceTypeExp:ExpressionContext, inputExpr:ExpressionContext):Seq[FhirPathResult] = {
    val resourceType = getStringValueOfExpr(resourceTypeExp, s"Invalid function call 'getHashedId', given expression for keyExpr:${resourceTypeExp.getText} should return a string value!")
    val input = getStringValueOfExpr(inputExpr, s"Invalid function call 'getHashedId', given expression for keyExpr:${inputExpr.getText} should return a string value!")
    Seq(FhirPathString(FhirMappingUtility.getHashedId(resourceType, input)))
  }

  private def getStringValueOfExpr(expr:ExpressionContext, errorMsg:String):String = {
    val result = new FhirPathExpressionEvaluator(context, current).visit(expr)
    if (result.length != 1 || !result.head.isInstanceOf[FhirPathString])
      throw new FhirPathException(errorMsg)
    result.head.asInstanceOf[FhirPathString].s
  }

  private def getStringValuesOfExpr(expr:ExpressionContext, errorMsg:String):Seq[String] = {
    val result = new FhirPathExpressionEvaluator(context, current).visit(expr)
    if (!result.forall(_.isInstanceOf[FhirPathString]))
      throw new FhirPathException(errorMsg)
    result.map(_.asInstanceOf[FhirPathString].s)
  }

  /**
   * Create a FHIR Reference object with given resource type and hash of the given id
   * @param resourceTypeExp Expression that will return resource type
   * @param inputExpr       Expression to return the value of referenced id
   * @return
   */
  @FhirPathFunction(documentation = "Creates a FHIR Reference object with given resource type and hash of the given id.Resource name should be quoted and id should be string. Ex: mpp:createFhirReferenceWithHashedId('Encounter', id.toString())",
    insertText = "mpp:createFhirReferenceWithHashedId(<resourceName>, <id>)",detail = "mpp", label = "mpp:createFhirReferenceWithHashedId", kind = "Function", returnType = Seq(), inputType = Seq())
  def createFhirReferenceWithHashedId(resourceTypeExp:ExpressionContext, inputExpr:ExpressionContext):Seq[FhirPathResult] = {
    val resourceType = getStringValueOfExpr(resourceTypeExp, s"Invalid function call 'createFhirReferenceWithHashedId', given expression for keyExpr:${resourceTypeExp.getText} should return a string value!")
    val input = getStringValuesOfExpr(inputExpr, s"Invalid function call 'createFhirReferenceWithHashedId', given expression for keyExpr:${inputExpr.getText} should return string value(s)!")
    input.map(inp => FhirPathComplex(JObject("reference" -> JString(FhirMappingUtility.getHashedReference(resourceType, inp)))))
  }

  /**
   * Creates a sequence of indices between from-to integers and concatenates them and prefix string to generate looped
   * field names (i.e. prefix+from,...,prefix+to). After that, for each field, it checks whether it has a value or not and returns the list of field names
   * which have values i.e the non-empty ones.
   *
   * @param prefixExpr Prefix string to be used to generate field name
   * @param fromExpr Starting index
   * @param toExpr   End index (inclusive)
   * @return the field names which have values i.e. the non-empty ones
   */
  @FhirPathFunction(documentation = "It populates field names by concatenating indices (from..to) to the prefix and" +
    " returns the ones which have a value. Ex: mpp:nonEmptyLoopedFields('child_',1,5)",
    insertText = "mpp:nonEmptyLoopedFields(<prefixExpr>, <fromExpr>, <toExpr>)",detail = "mpp", label = "mpp:nonEmptyLoopedFields", kind = "Function",
    returnType = Seq("String"), inputType = Seq())
  def nonEmptyLoopedFields(prefixExpr: ExpressionContext, fromExpr: ExpressionContext, toExpr: ExpressionContext): Seq[FhirPathResult] = {
    val prefix = new FhirPathExpressionEvaluator(context, current).visit(prefixExpr)
    if (prefix.length != 1 || !prefix.forall(_.isInstanceOf[FhirPathString]))
      throw new FhirPathException(s"Invalid function call 'nonEmptyLoopedFields', 'prefix' expression should return a string value!")

    val from = new FhirPathExpressionEvaluator(context, current).visit(fromExpr)
    if (from.length != 1 || !from.forall(_.isInstanceOf[FhirPathNumber]))
      throw new FhirPathException(s"Invalid function call 'nonEmptyLoopedFields', 'from' expression should return a integer value!")

    val to = new FhirPathExpressionEvaluator(context, current).visit(toExpr)
    if (to.length != 1 || !to.forall(_.isInstanceOf[FhirPathNumber]))
      throw new FhirPathException(s"Invalid function call 'nonEmptyLoopedFields', 'to' expression should return a integer value!")

    current
      .map(_.asInstanceOf[FhirPathComplex])
      .flatMap(r => {
        val prefixString = prefix.head.asInstanceOf[FhirPathString].s

        (from.head.asInstanceOf[FhirPathNumber].v.toInt to to.head.asInstanceOf[FhirPathNumber].v.toInt) // create indices
          .map(i => prefixString + i.toString) // concatenate prefix and index
          .filter(p => { // filter non-empty fields
            // all data types except boolean can be extracted as string
            val stringValue = FHIRUtil.extractValueOption[String](r.json, p)

            if(stringValue.nonEmpty){
              // handle the empty string
              stringValue.get.nonEmpty
            } else {
              // field can be a boolean, handle it
              val booleanValue = FHIRUtil.extractValueOption[Boolean](r.json, p)
              booleanValue.nonEmpty
            }
          })
      })
      .map(x => FhirPathString(x))
  }

  /**
   * You can also get the whole concept columns as complex Json object if there is more than one columns,
   * if there is only one target column it just returns the value of it as string
   * @param conceptMap  This should be reference to the conceptMap context e.g. %obsConceptMap
   * @param keyExpr     This is any expression that will provide the key value e.g. code
   * @return
   */
  @FhirPathFunction(documentation = "Returns concept from concept map.If there are more than one target column, it returns them as a complex Json object. Ex: mpp:getConcept(%prsConceptMap, code)",
    insertText = "mpp:getConcept(<%conceptMap>, <keyExpr>)",detail = "mpp", label = "mpp:getConcept", kind = "Function", returnType = Seq(), inputType = Seq())
  def getConcept(conceptMap: ExpressionContext, keyExpr: ExpressionContext): Seq[FhirPathResult] = {
    val mapName = conceptMap.getText.substring(1) // skip the leading % character
    val conceptMapContext = getConceptMap(mapName)
    val evaluator = new FhirPathExpressionEvaluator(context, current)
    // Should return the code of the concept whose mapping is requested
    evaluator.visit(keyExpr) match {
      case Nil =>
        Nil
      case Seq(FhirPathString(conceptCode)) =>
        conceptMapContext
          .concepts
          .get(conceptCode)
          .map {
            case mws:Map[String, String] if mws.size == 1 => FhirPathString(mws.values.head)
            case mws =>
              FhirPathComplex(JObject(mws.toList.map(i => i._1 -> JString(i._2))))
          }
          .toSeq
      case _ =>
        throw new FhirPathException(s"Invalid function call 'getConcept', given expression for keyExpr:${keyExpr.getText} for the concept code should return a string value!")
    }
  }

  /**
   * Load the concept map
   *
   * @param name name of the map
   * @return
   */
  private def getConceptMap(name: String): ConceptMapContext =
    try {
      mappingContext(name).asInstanceOf[ConceptMapContext]
    } catch {
      case e: Exception =>
        throw new FhirPathException(s"Invalid function call 'getConcept', given expression for conceptMap:%$name should point to a valid map entry in the provided mapping context!")
    }

  /**
   * Get corresponding value from the given concept map with the given key and column name
   * If there is no concept with given key, return Nil
   * e.g. getConcept(%obsConceptMap, code, 'source_system')
   *
   * @param conceptMap This should be reference to the conceptMap context e.g. %obsConceptMap
   * @param keyExpr    This is any expression that will provide the key value e.g. code
   * @param columnName This should be the FHIR Path string literal providing the name of the column
   * @return
   */
  @FhirPathFunction(documentation = "Returns concept from concept map. Ex: mpp:getConcept(%prsConceptMap, %ind, 'target_code')",insertText = "mpp:getConcept(<%conceptMap>, <keyExpr>, <columnName>)",detail = "mpp", label = "mpp:getConcept"
    , kind = "Function", returnType = Seq("string"), inputType = Seq())
  def getConcept(conceptMap: ExpressionContext, keyExpr: ExpressionContext, columnName: ExpressionContext): Seq[FhirPathResult] = {
    val mapName = conceptMap.getText.substring(1) // skip the leading % character
    val conceptMapContext = getConceptMap(mapName)

    val targetFieldResult = new FhirPathExpressionEvaluator(context, current).visit(columnName)
    if (targetFieldResult.length != 1 || !targetFieldResult.head.isInstanceOf[FhirPathString]) {
      throw new FhirPathException(s"Invalid function call 'getConcept', given expression for columnName:${columnName.getText} for the target field should return a string value!")
    }
    val targetField = targetFieldResult.head.asInstanceOf[FhirPathString].s

    val conceptCodeResult = new FhirPathExpressionEvaluator(context, current).visit(keyExpr) // Should return the code of the concept whose mapping is requested
    if (conceptCodeResult.length > 1 || !conceptCodeResult.forall(_.isInstanceOf[FhirPathString])) {
      throw new FhirPathException(s"Invalid function call 'getConcept', given expression for keyExpr:${keyExpr.getText} for the concept code should return a string value!")
    }
    //If conceptCode returns empty, also return empty, if there is no such key or target column is null also return empty
    val result =
      conceptCodeResult
      .headOption
      .map(_.asInstanceOf[FhirPathString].s) match {
        case None => Nil
        case Some(conceptCode) =>
          conceptMapContext
            .concepts
            .get(conceptCode)
            .flatMap(codeEntry =>
              codeEntry.get(targetField).filter(_ != "")
            )
            .map(mappedValue =>  FhirPathString(mappedValue))
            .toSeq
    }
    result
  }

  /**
   * Convert the given value in given unit to the target unit specified in the context file with specified conversion function, return FHIR Path Quantity
   * If there is no corresponding key (code) or unit in the context map, then return Nil
   *
   * @param conversionFunctionsMap Map of conversion functions for given code and unit
   * @param keyExpr                FHIR Path expression returning the key value (code)
   * @param valueExpr              FHIR Path expression returning the value in the source
   * @param unitExpr               FHIR Path expression returning the unit in the source
   * @return
   */
  @FhirPathFunction(documentation = "Converts the given value in given unit to the target unit specified in the context file with specified conversion function, return FHIR Path Quantity.\nEx: mpp:convertAndReturnQuantity(%conversionFunctions, %ind, %ind, %ind)",
    insertText = "mpp:convertAndReturnQuantity(<%conversionFunctions>, <keyExpr>, <valueExpr>, <unitExpr>)",detail = "mpp", label = "mpp:convertAndReturnQuantity", kind = "Function", returnType = Seq(), inputType = Seq())
  def convertAndReturnQuantity(conversionFunctionsMap: ExpressionContext, keyExpr: ExpressionContext, valueExpr: ExpressionContext, unitExpr: ExpressionContext): Seq[FhirPathResult] = {
    val mapName = conversionFunctionsMap.getText.substring(1) // skip the leading % character
    val unitConversionContext = try {
      mappingContext(mapName).asInstanceOf[UnitConversionContext]
    } catch {
      case e: Exception => throw new FhirPathException(s"Invalid function call 'convertAndReturnQuantity', given expression for conversionFunctionsMap:${conversionFunctionsMap.getText} should point to a valid map entry in the provided mapping context!")
    }

    val codeResult = new FhirPathExpressionEvaluator(context, current).visit(keyExpr)
    if (codeResult.length > 1 || !codeResult.head.isInstanceOf[FhirPathString]) {
      throw new FhirPathException(s"Invalid function call 'convertAndReturnQuantity', given expression for keyExpr:${keyExpr.getText} for the source code should return a string value!")
    }
    //Handle value expression
    var valueResult = new FhirPathExpressionEvaluator(context, current).visit(valueExpr)
    val valueAndComparator = new FhirPathExpressionEvaluator(context, valueResult).visit(FhirPathEvaluator.parse("utl:parseFhirQuantityExpression($this)"))
    valueResult = valueAndComparator.headOption.toSeq
    if (valueResult.length > 1 || !valueResult.head.isInstanceOf[FhirPathNumber]) {
      throw new FhirPathException(s"Invalid function call 'convertAndReturnQuantity', given expression for valueExpr:${valueExpr.getText} for the value should return a numeric value!")
    }
    val comparator = valueAndComparator.drop(1).headOption.map(_.asInstanceOf[FhirPathString].s)

    val unitResult = new FhirPathExpressionEvaluator(context, current).visit(unitExpr)
    if (unitResult.length != 1 || !unitResult.head.isInstanceOf[FhirPathString]) {
      throw new FhirPathException(s"Invalid function call 'convertAndReturnQuantity', given expression for unitExpr:${unitExpr.getText} for the source unit should return a string value!")
    }
    val unit = unitResult.head.asInstanceOf[FhirPathString].s

    if(codeResult.isEmpty || valueResult.isEmpty)
      Nil
    else {
      val code = codeResult.head.asInstanceOf[FhirPathString].s
      unitConversionContext
        .conversionFunctions
        .get(code -> unit)
        .map {
          case (targetUnit, conversionFunction) =>
            val conversionFunctionExpressionContext = FhirPathEvaluator.parse(conversionFunction)
            val functionResult = new FhirPathExpressionEvaluator(context, valueResult).visit(conversionFunctionExpressionContext)
            if (functionResult.length != 1 || !functionResult.head.isInstanceOf[FhirPathNumber]) {
              throw new FhirPathException(s"Invalid FHIR expression in the unit conversion context! The FHIR path expression:${conversionFunction} should evaluate to a single numeric value!")
            }
            FhirPathComplex(JObject(List(
              "value" -> functionResult.head.toJson,
              "system" -> JString("http://unitsofmeasure.org"),
              "unit" -> JString(targetUnit),
              "code" -> JString(targetUnit)
            ) ++
              comparator.map(c => "comparator" -> JString(c)).toList,
            ))
        }.toSeq
    }
  }
}

class FhirMappingFunctionsFactory(mappingContext: Map[String, FhirMappingContext]) extends IFhirPathFunctionLibraryFactory with Serializable {
  override def getLibrary(context: FhirPathEnvironment, current: Seq[FhirPathResult]): AbstractFhirPathFunctionLibrary =
    new FhirPathMappingFunctions(context, current, mappingContext)
}
