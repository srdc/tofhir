package io.tofhir.common.util

import io.onfhir.api.FHIR_DATA_TYPES
import io.onfhir.path.annotation.{FhirPathFunction, FhirPathFunctionDocumentation, FhirPathFunctionParameter, FhirPathFunctionReturn}
import io.onfhir.path.grammar.FhirPathExprParser.ExpressionContext
import io.onfhir.path.{AbstractFhirPathFunctionLibrary, FhirPathEnvironment, FhirPathException, FhirPathExpressionEvaluator, FhirPathResult, FhirPathString, IFhirPathFunctionLibraryFactory}

import java.nio.ByteBuffer
import java.util
import java.util.Base64

/**
 * Function library containing custom, project-specific functions
 * TODO we might consider adding such functions as dedicated libraries
 *
 * @param context FhirPathContext
 * @param current Current evaluated FhirPath result (the function will execute on this results)
 */
class CustomMappingFunctions(context: FhirPathEnvironment, current: Seq[FhirPathResult]) extends AbstractFhirPathFunctionLibrary with Serializable {

  /**
   * Decodes the given data and converts it to an array of space separated numbers e.g '123 456 123'.
   *
   * @param dataExpr String data such that byte representation of each 2 consecutive characters represents a number.
   * @return Space separated numbers concatenated in a string
   */
  @FhirPathFunction(
    documentation = FhirPathFunctionDocumentation(
      detail = "Decodes the given data and converts it to an array of space separated numbers. Returns the space separated numbers concatenated in a string.",
      usageWarnings = None,
      parameters = Some(Seq(
        FhirPathFunctionParameter(
          name = "dataExpr",
          detail = "String data such that byte representation of each 2 consecutive characters represents a number.",
          examples = None
        )
      )),
      returnValue = FhirPathFunctionReturn(
        detail = None,
        examples = Seq("'123 456 123'")
      ),
      examples = Seq(
        "cst:createTimeSeriesData(%data)"
      )
    ),
    insertText = "cst:createTimeSeriesData(<dataExpr>)",
    detail = "cst",
    label = "cst:createTimeSeriesData",
    kind = "Method",
    returnType = Seq(FHIR_DATA_TYPES.STRING),
    inputType = Seq(FHIR_DATA_TYPES.STRING)
  )
  def createTimeSeriesData(dataExpr: ExpressionContext): Seq[FhirPathResult] = {
    val dataResult = new FhirPathExpressionEvaluator(context, current).visit(dataExpr)
    if (dataResult.length > 1 || !dataResult.head.isInstanceOf[FhirPathString]) {
      throw new FhirPathException(s"Invalid function call 'createTimeSeriesData', given expression for origin parameter: ${dataExpr.getText} should return a single, string value!")
    }

    val data: String = dataResult.head.asInstanceOf[FhirPathString].s
    val encoded: Array[Byte] = Base64.getEncoder().encode(data.getBytes());
    val decodedNumbers: Seq[String] = Range(0, encoded.length, 2).map(i => {
      val bf: ByteBuffer = ByteBuffer.wrap(util.Arrays.copyOfRange(encoded, i, i + 2)).order(java.nio.ByteOrder.LITTLE_ENDIAN)
      val decoded: Double = bf.getShort
      decoded + ""
    })
    Seq(FhirPathString(decodedNumbers.mkString(" ")))
  }
}

class CustomMappingFunctionsFactory() extends IFhirPathFunctionLibraryFactory with Serializable {
  override def getLibrary(context: FhirPathEnvironment, current: Seq[FhirPathResult]): AbstractFhirPathFunctionLibrary =
    new CustomMappingFunctions(context, current)
}
