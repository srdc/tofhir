package io.tofhir.rxnorm

import io.onfhir.api.util.FHIRUtil
import io.onfhir.path.annotation.FhirPathFunction
import io.onfhir.path.grammar.FhirPathExprParser.ExpressionContext
import io.onfhir.path.{AbstractFhirPathFunctionLibrary, FhirPathComplex, FhirPathEnvironment, FhirPathException, FhirPathExpressionEvaluator, FhirPathNumber, FhirPathResult, FhirPathString, IFhirPathFunctionLibraryFactory}
import org.json4s.JsonAST.{JArray, JDouble, JObject, JString}

/**
 * FHIR Path Function library to access RxNorm API functionalities using them for mappings for medication concepts
 * @param rxNormApiRootUrl  Root URL for RxNorm API
 * @param timeoutInSec      Timeout for API calls
 * @param context           FHIR Path context
 * @param current           Current FHIR Path input context
 * @param actorSystem       Akka Actor System
 */
class RxNormApiFunctionLibrary(rxNormApiClient:RxNormApiClient, context: FhirPathEnvironment, current: Seq[FhirPathResult]) extends AbstractFhirPathFunctionLibrary with Serializable {
  val evaluator = new FhirPathExpressionEvaluator(context, current)
  /**
   *
   * @param ndcExpr
   * @return
   */
   @FhirPathFunction(
     documentation = "\uD83D\uDCDC Finds out RxNorm Concept Ids (rxcui) for the given NDC code.\n\n\uD83D\uDCDD <span style=\"color:#ff0000;\">_@param_</span> **`ndcExpr`**  \nThe NDC code expression.\n\n\uD83D\uDD19 <span style=\"color:#ff0000;\">_@return_</span>  \n```\n[\"104922\", \"105784\"]\n``` \n\uD83D\uDCA1 **E.g.** rxn:findRxConceptIdsByNdc('12345678901')",
     insertText = "rxn:findRxConceptIdsByNdc(<ndcCode>)",
     detail = "rxn",
     label = "rxn:findRxConceptIdByNdc",
     kind = "Function",
     returnType = Seq("string"),
     inputType = Seq("string")
   )
  def findRxConceptIdsByNdc(ndcExpr:ExpressionContext):Seq[FhirPathResult] = {
     val ndc: Option[String] =
       evaluator.visit(ndcExpr) match {
         case Seq(FhirPathString(s)) => Some(s)
         case Seq(FhirPathNumber(v)) =>
           Some((0 until  (11 - v.toLong.toString().length)).map(_ => "0").mkString  + v.toLong.toString())
         case Nil => None
         case _ => throw new FhirPathException("Invalid parameter ndc! It should evaluate a single string")
       }
     val result =
       ndc
        .map(rxNormApiClient.findRxConceptIdByNdc)
        .getOrElse(Nil)
        .map(FhirPathString)
     result
  }

  /**
   *
   * @param rxcuiExpr
   * @return
   */
  @FhirPathFunction(
    documentation = "\uD83D\uDCDC Finds out Ingredients and their properties for the given RxNorm Drug.\n\n\uD83D\uDCDD <span style=\"color:#ff0000;\">_@param_</span> **`rxcuiExpr`**  \nThe RxNorm Concept Id expression for the drug.\n\n\uD83D\uDD19 <span style=\"color:#ff0000;\">_@return_</span>  \n```json\n{\n  \"ingredientAndStrength\": [\n    {\n      \"activeIngredientRxcui\": \"104922\",\n      \"activeIngredientName\": \"Acetaminophen\",\n      \"numeratorValue\": 500.0,\n      \"numeratorUnit\": \"mg\",\n      \"denominatorValue\": 1.0,\n      \"denominatorUnit\": \"tablet\"\n    },\n    {\n      \"activeIngredientRxcui\": \"105784\",\n      \"activeIngredientName\": \"Ibuprofen\",\n      \"numeratorValue\": 200.0,\n      \"numeratorUnit\": \"mg\",\n      \"denominatorValue\": 1.0,\n      \"denominatorUnit\": \"tablet\"\n    }\n  ],\n  \"doseFormConcept\": {\n    \"rxcui\": \"106366\",\n    \"doseFormName\": \"Tablet\"\n  }\n}\n``` \n\uD83D\uDCA1 **E.g.** rxn:getMedicationDetails('12345')",
    insertText = "rxn:getMedicationDetails(<rxcui>)",
    detail = "rxn",
    label = "rxn:getMedicationDetails",
    kind = "Function",
    returnType = Seq("complex"),
    inputType = Seq("string")
  )
  def getMedicationDetails(rxcuiExpr:ExpressionContext):Seq[FhirPathResult] = {
    val conceptIds = evaluator.visit(rxcuiExpr)
    if(!conceptIds.forall(_.isInstanceOf[FhirPathString]))
      throw new FhirPathException("Invalid parameter rxcui! It should evaluate to 0 or more string")
    val rxcuis = conceptIds.map(_.asInstanceOf[FhirPathString].s)
    //Go over all the concepts and find the first one that have information we are looking for
    rxcuis
      .iterator
      .flatMap(rxcui =>
        rxNormApiClient.getRxcuiHistoryStatus(rxcui)
      )
      .map(response => {
        // Get the ingredient details
        val ingredients =
          FHIRUtil
            .extractValueOptionByPath[Seq[JObject]](response, "rxcuiStatusHistory.definitionalFeatures.ingredientAndStrength")
            .getOrElse(Nil)
        val ingredientObjs =
          ingredients
            .map(i => {
              val requiredFields =
                Seq("activeIngredientRxcui", "activeIngredientName", "numeratorValue", "numeratorUnit", "denominatorValue", "denominatorUnit")
                  .map(f =>
                    FHIRUtil.extractValueOption[String](i, f)
                      .filter(_ != "")  // Should filter empty string values
                      .map(v =>
                        if(f == "numeratorValue" || f == "denominatorValue")
                          f -> JDouble(v.toDouble)
                        else
                          f ->   JString(v)
                      )
                  )
              if (requiredFields.forall(_.isDefined))
                Some(JObject(requiredFields.map(_.get).toList))
              else
                None
            })

        val doseFormObj =
            FHIRUtil
            .extractValueOptionByPath[Seq[JObject]](response, "rxcuiStatusHistory.definitionalFeatures.doseFormConcept")
            .getOrElse(Nil)
            .headOption
        if(ingredientObjs.nonEmpty && ingredientObjs.forall(_.isDefined)){
          Some(
            JObject(
              List(
                "ingredientAndStrength" -> JArray(ingredientObjs.map(_.get).toList)
              ) ++
                doseFormObj
                  .map(d => "doseFormConcept" -> d)
                  .toSeq
            )
          )
        } else
          None
      })
      .find(_.isDefined)
      .map(r => FhirPathComplex(r.get))
      .toList
  }


  /**
   *
   * @param rxcuiExpr
   * @return
   */
  @FhirPathFunction(
    documentation = "\uD83D\uDCDC Finds out Ingredients and their properties for the given RxNorm Drug.\n\n\uD83D\uDCDD <span style=\"color:#ff0000;\">_@param_</span> **`rxcuiExpr`**  \nThe RxNorm Concept Id expression for the drug.\n\n\uD83D\uDD19 <span style=\"color:#ff0000;\">_@return_</span>  \n```json\n[\n  {\n    \"ingredient\": \"Acetaminophen\",\n    \"strength\": \"500 mg\"\n  },\n  {\n    \"ingredient\": \"Ibuprofen\",\n    \"strength\": \"200 mg\"\n  }\n]\n``` \n\uD83D\uDCA1 **E.g.** rxn:findIngredientsOfDrug('12345')",
    insertText = "rxn:findIngredientsOfDrug(<rxcui>)",
    detail = "rxn",
    label = "rxn:findIngredientsOfDrug",
    kind = "Function",
    returnType = Seq("complex"),
    inputType = Seq("string")
  )
  def findIngredientsOfDrug(rxcuiExpr:ExpressionContext):Seq[FhirPathResult] = {
    val rxcui: Option[String] =
      evaluator.visit(rxcuiExpr) match {
        case Seq(FhirPathString(s)) => Some(s)
        case Nil => None
        case _ => throw new FhirPathException("Invalid parameter rxcui! It should evaluate to a single string")
      }
    rxcui
      .map(rxNormApiClient.getIngredientProperties)
      .map(_.map(FhirPathComplex))
      .getOrElse(Nil)
  }

  /**
   * Get the corresponding ATC code for given RxNorm ingredient concept
   * @param rxcuiExpr Concept id
   * @return
   */
  @FhirPathFunction(
    documentation = "\uD83D\uDCDC Finds the corresponding ATC code for the given RxNorm ingredient concept.\n\n\uD83D\uDCDD <span style=\"color:#ff0000;\">_@param_</span> **`rxcuiExpr`**  \nThe RxNorm Concept Id expression for the drug.\n\n\uD83D\uDD19 <span style=\"color:#ff0000;\">_@return_</span>  \n```\n\"N02BE01\"\n``` \n\uD83D\uDCA1 **E.g.** rxn:getATC('12345')",
    insertText = "rxn:getATC(<rxcui>)",
    detail = "rxn",
    label = "rxn:getATC",
    kind = "Function",
    returnType = Seq("string"),
    inputType = Seq("string")
  )
  def getATC(rxcuiExpr:ExpressionContext): Seq[FhirPathResult] = {
    val rxcui: String =
      evaluator.visit(rxcuiExpr) match {
        case Seq(FhirPathString(s)) => s
        case _ => throw new FhirPathException("Invalid parameter rxcui! It should evaluate to a single string")
      }
    rxNormApiClient.getAtcCode(rxcui)
      .map(FhirPathString)
  }
}

/**
 * Factory class to generate function library
 * @param rxNormApiRootUrl  Root URL for RxNorm API e.g.  https://rxnav.nlm.nih.gov
 * @param timeoutInSec      Timeout in seconds for API calls
 * @param actorSystem       Akka actor system for HTTP calls
 */
class RxNormApiFunctionLibraryFactory(rxNormApiRootUrl:String, timeoutInSec:Int/*config:Config*/) extends IFhirPathFunctionLibraryFactory with Serializable {
  //val rxNormApiRootUrl = config.getString("api.root-url")
  //val timeoutInSec = config.getInt("api.timeout")
  val rxNormApiClient = RxNormApiClient(rxNormApiRootUrl, timeoutInSec)
  override def getLibrary(context: FhirPathEnvironment,  current: Seq[FhirPathResult]): AbstractFhirPathFunctionLibrary =
    new RxNormApiFunctionLibrary(rxNormApiClient, context, current)
}