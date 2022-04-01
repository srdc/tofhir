package io.onfhir.tofhir.engine

import io.onfhir.path.grammar.FhirPathExprParser.ExpressionContext
import io.onfhir.path.{AbstractFhirPathFunctionLibrary, FhirPathEnvironment, FhirPathExpressionEvaluator, FhirPathResult, IFhirPathFunctionLibraryFactory}
import io.onfhir.tofhir.model.FhirMappingContext

/**
 * Function library for FHIR Path expressions that provide mapping utility functions
 * @param context           FHIR Path context
 * @param current           Current result to apply the function on
 * @param mappingContext    Specific mapping context
 */
class FhirPathMappingFunctions(context:FhirPathEnvironment, current:Seq[FhirPathResult], mappingContext:Map[String, FhirMappingContext])
  extends AbstractFhirPathFunctionLibrary {

  /**
   * Get corresponding value from the given concept map with the given key and column name
   * e.g. getConcept(%obsConceptMap, code, 'source_system')
   * @param conceptMap    This should be reference to the conceptMap context e.g. %obsConceptMap
   * @param keyExpr       This is any expression that will provide the key value e.g. code
   * @param columnName    This should be the FHIR Path string literal providing the name of the column
   * @return
   */
  def getConcept(conceptMap:ExpressionContext, keyExpr:ExpressionContext, columnName:ExpressionContext):Seq[FhirPathResult] = {
    throw new NotImplementedError()
  }

  /**
   * Convert the value in given unit to the target unit with specified conversion function given in conversionFunctionsMap
   * @param conversionFunctionsMap    Map of conversion functions for given code and unit
   * @param keyExpr                   FHIR Path expression returning the key value (code)
   * @param sourceUnitExpr            FHIR Path expression returning the unit
   * @return
   */
  def convertUnit(conversionFunctionsMap:ExpressionContext, keyExpr:ExpressionContext, sourceUnitExpr:ExpressionContext):Seq[FhirPathResult] = {
    throw new NotImplementedError()
  }

}

class FhirMappingFunctionsFactory(mappingContext:Map[String, FhirMappingContext]) extends IFhirPathFunctionLibraryFactory {
  override def getLibrary(context: FhirPathEnvironment, current: Seq[FhirPathResult]): AbstractFhirPathFunctionLibrary =
    new FhirPathMappingFunctions(context, current, mappingContext)
}