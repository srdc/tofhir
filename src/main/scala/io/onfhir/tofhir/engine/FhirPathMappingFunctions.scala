package io.onfhir.tofhir.engine

import com.google.common.hash.Hashing
import io.onfhir.path.grammar.FhirPathExprParser.ExpressionContext
import io.onfhir.path._
import io.onfhir.tofhir.model.{ConceptMapContext, FhirMappingContext, FhirMappingException, UnitConversionContext}
import io.onfhir.tofhir.util.FhirMappingUtility
import org.apache.commons.codec.binary.StringUtils
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
  def createFhirReferenceWithHashedId(resourceTypeExp:ExpressionContext, inputExpr:ExpressionContext):Seq[FhirPathResult] = {
    val resourceType = getStringValueOfExpr(resourceTypeExp, s"Invalid function call 'createFhirReferenceWithHashedId', given expression for keyExpr:${resourceTypeExp.getText} should return a string value!")
    val input = getStringValuesOfExpr(inputExpr, s"Invalid function call 'createFhirReferenceWithHashedId', given expression for keyExpr:${inputExpr.getText} should return string value(s)!")
    input.map(inp => FhirPathComplex(JObject("reference" -> JString(FhirMappingUtility.getHashedReference(resourceType, inp)))))
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
  def getConcept(conceptMap: ExpressionContext, keyExpr: ExpressionContext, columnName: ExpressionContext): Seq[FhirPathResult] = {
    val mapName = conceptMap.getText.substring(1) // skip the leading % character
    val conceptMapContext = try {
      mappingContext(mapName).asInstanceOf[ConceptMapContext]
    } catch {
      case e: Exception => throw new FhirPathException(s"Invalid function call 'getConcept', given expression for conceptMap:${conceptMap.getText} should point to a valid map entry in the provided mapping context!")
    }

    val conceptCodeResult = new FhirPathExpressionEvaluator(context, current).visit(keyExpr) // Should return the code of the concept whose mapping is requested
    if (conceptCodeResult.length != 1 || !conceptCodeResult.head.isInstanceOf[FhirPathString]) {
      throw new FhirPathException(s"Invalid function call 'getConcept', given expression for keyExpr:${keyExpr.getText} for the concept code should return a string value!")
    }
    val conceptCode = conceptCodeResult.head.asInstanceOf[FhirPathString].s

    val targetFieldResult = new FhirPathExpressionEvaluator(context, current).visit(columnName)
    if (targetFieldResult.length != 1 || !targetFieldResult.head.isInstanceOf[FhirPathString]) {
      throw new FhirPathException(s"Invalid function call 'getConcept', given expression for columnName:${columnName.getText} for the target field should return a string value!")
    }
    val targetField = targetFieldResult.head.asInstanceOf[FhirPathString].s

    val codeEntry = try {
      conceptMapContext.concepts(conceptCode)
    } catch {
      case e: NoSuchElementException =>
        throw FhirMappingException(s"Concept code:$conceptCode cannot be found in the ConceptMapContext:$mapName", e)
    }

    val mappedValue = try {
      codeEntry(targetField)
    } catch {
      case e: NoSuchElementException =>
        throw FhirMappingException(s"For the given concept code:$conceptCode, the column:$targetField cannot be " +
          s"found in the ConceptMapContext:$mapName. Available columns are ${conceptMapContext.concepts.head._2.keySet.mkString(",")}", e)
    }

    Seq(FhirPathString(mappedValue))
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
  def convertAndReturnQuantity(conversionFunctionsMap: ExpressionContext, keyExpr: ExpressionContext, valueExpr: ExpressionContext, unitExpr: ExpressionContext): Seq[FhirPathResult] = {
    val mapName = conversionFunctionsMap.getText.substring(1) // skip the leading % character
    val unitConversionContext = try {
      mappingContext(mapName).asInstanceOf[UnitConversionContext]
    } catch {
      case e: Exception => throw new FhirPathException(s"Invalid function call 'convertAndReturnQuantity', given expression for conversionFunctionsMap:${conversionFunctionsMap.getText} should point to a valid map entry in the provided mapping context!")
    }

    val codeResult = new FhirPathExpressionEvaluator(context, current).visit(keyExpr)
    if (codeResult.length != 1 || !codeResult.head.isInstanceOf[FhirPathString]) {
      throw new FhirPathException(s"Invalid function call 'convertAndReturnQuantity', given expression for keyExpr:${keyExpr.getText} for the source code should return a string value!")
    }
    val code = codeResult.head.asInstanceOf[FhirPathString].s

    val valueResult = new FhirPathExpressionEvaluator(context, current).visit(valueExpr)
    if (valueResult.length != 1 || !valueResult.head.isInstanceOf[FhirPathNumber]) {
      throw new FhirPathException(s"Invalid function call 'convertAndReturnQuantity', given expression for valueExpr:${valueExpr.getText} for the value should return a numeric value!")
    }

    val unitResult = new FhirPathExpressionEvaluator(context, current).visit(unitExpr)
    if (unitResult.length != 1 || !unitResult.head.isInstanceOf[FhirPathString]) {
      throw new FhirPathException(s"Invalid function call 'convertAndReturnQuantity', given expression for unitExpr:${unitExpr.getText} for the source unit should return a string value!")
    }
    val unit = unitResult.head.asInstanceOf[FhirPathString].s

    val (targetUnit, conversionFunction) = try {
      unitConversionContext.conversionFunctions(code -> unit)
    } catch {
      case e: NoSuchElementException =>
        throw FhirMappingException(s"(code, unit) pair:($code, $unit) cannot be found in the UnitConversionFunctionsContext:$mapName", e)
    }
    val conversionFunctionExpressionContext = FhirPathEvaluator.parse(conversionFunction)
    val functionResult = new FhirPathExpressionEvaluator(context, valueResult).visit(conversionFunctionExpressionContext)
    if (functionResult.length != 1 || !functionResult.head.isInstanceOf[FhirPathNumber]) {
      throw new FhirPathException(s"Invalid FHIR expression in the unit conversion context! The FHIR path expression:${conversionFunction} should evaluate to a single numeric value!")
    }

    Seq(FhirPathComplex(JObject(List("value" -> functionResult.head.toJson, "system" -> JString("http://unitsofmeasure.org"), "unit" -> JString(targetUnit), "code" -> codeResult.head.toJson))))
  }

}

class FhirMappingFunctionsFactory(mappingContext: Map[String, FhirMappingContext]) extends IFhirPathFunctionLibraryFactory with Serializable {
  override def getLibrary(context: FhirPathEnvironment, current: Seq[FhirPathResult]): AbstractFhirPathFunctionLibrary =
    new FhirPathMappingFunctions(context, current, mappingContext)
}
