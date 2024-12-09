package io.tofhir.rxnorm

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.unmarshalling.{Unmarshal, Unmarshaller}
import io.onfhir.api.Resource
import io.onfhir.api.util.FHIRUtil
import io.onfhir.path.FhirPathException
import io.onfhir.util.JsonFormatter._
import org.json4s.JsonAST.{JDouble, JObject, JString}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

case class RxNormApiClient(rxNormApiRootUrl:String, timeoutInSec:Int) {

  /**
   * Find out RxNorm Concept Id (rxcui) for given NDC code (active or historic)
   * See https://lhncbc.nlm.nih.gov/RxNav/APIs/api-RxNorm.getNDCProperties.html
   * @param ndc NDC (National drug classification) code
   * @return  Corresponding RxNorm concept id
   */
  def findRxConceptIdByNdc(ndc:String):Seq[String] = {
    val uri = Uri.apply(s"$rxNormApiRootUrl/REST/ndcproperties.json")
    val request =
      HttpRequest
        .apply(
          HttpMethods.GET,
          uri = uri.withQuery(Uri.Query.apply(Map("id" -> ndc, "ndcstatus" -> "ALL")))
        )

    executeRequest(request)
      .flatMap(response =>
        FHIRUtil
          .extractValueOptionByPath[Seq[String]](response, "ndcPropertyList.ndcProperty.rxcui")
          .map(_.filter(r => r != "" && r != "/"))
      )
      .getOrElse(Nil)
  }

  /**
   * Find out RxNorm Concept Id (rxcui) for given drug name
   * See https://lhncbc.nlm.nih.gov/RxNav/APIs/api-RxNorm.findRxcuiByString.html
   * @param drug
   * @return
   */
  def findRxConceptIdByName(drug:String):Option[String] = {
    val request =
      HttpRequest
        .apply(
          HttpMethods.GET,
          uri = Uri.apply(s"$rxNormApiRootUrl/REST/rxcui.json?name=${drug}&search=2")
        )

    executeRequest(request)
      .flatMap(response =>
        FHIRUtil
          .extractValueOptionByPath[Seq[String]](response, "idGroup.rxnormId")
          .flatMap(_.headOption)
      )
  }

  /**
   * RxNorm API call for getRxcuiHistoryStatus
   * If concept is unknown, return None
   * See https://lhncbc.nlm.nih.gov/RxNav/APIs/api-RxNorm.getRxcuiHistoryStatus.html
   * @param rxcui RxNorm Concept Identifier
   * @return
   */
  def getRxcuiHistoryStatus(rxcui:String):Option[Resource] = {
    val request =
      HttpRequest
        .apply(
          HttpMethods.GET,
          uri = Uri.apply(s"$rxNormApiRootUrl/REST/rxcui/$rxcui/historystatus.json")
        )

    executeRequest(request)
      .filter(response => {
        //Check if this is concept is known (either active or obsolete)
        val status = FHIRUtil
          .extractValueOptionByPath[String](response, "rxcuiStatusHistory.metaData.status")
        status.exists(_ != "UNKNOWN")
      })
  }

  /**
   * Find out active ingredients and their basic properties for given RxNorm Clinical Drug Pack or component
   * See https://lhncbc.nlm.nih.gov/RxNav/APIs/api-RxNorm.getAllProperties.html
   * @param rxcui
   * @return
   */
  def getIngredientProperties(rxcui:String):Seq[Resource] = {
    val request =
      HttpRequest
        .apply(
          HttpMethods.GET,
          uri = Uri.apply(s"$rxNormApiRootUrl/REST/rxcui/${rxcui}/allProperties.json?prop=attributes")
        )

    executeRequest(request)
      .map(response =>
        FHIRUtil
          .extractValueOptionByPath[Seq[JObject]](response, "propConceptGroup.propConcept")
          .map(attrs => {
            val parsedProps =
              attrs
                .map(attr =>
                  FHIRUtil.extractValue[String](attr, "propName") ->
                    FHIRUtil.extractValue[String](attr, "propValue")
                )

            val ingredientCodes = parsedProps.filter(_._1 == "Active_ingredient_RxCUI").map(_._2)
            val ingredientNames = parsedProps.filter(_._1 == "Active_ingredient_name").map(_._2)
            val numeratorValues = parsedProps.filter(_._1 == "Numerator_Value").map(_._2)
            val numeratorUnits = parsedProps.filter(_._1 == "Numerator_Units").map(_._2)
            val denominatorValues = parsedProps.filter(_._1 == "Denominator_Value").map(_._2)
            val denominatorUnits = parsedProps.filter(_._1 == "Denominator_Units").map(_._2)

            ingredientCodes.indices.map(i =>
              JObject(List(
                "Active_ingredient_RxCUI" -> JString(ingredientCodes.apply(i)),
                "Active_ingredient_name" -> JString(ingredientNames.apply(i)),
                "Numerator_Value" -> JDouble(numeratorValues.apply(i).toDouble),
                "Numerator_Units" -> JString(numeratorUnits.apply(i)),
                "Denominator_Value" -> JDouble(denominatorValues.apply(i).toDouble),
                "Denominator_Units" -> JString(denominatorUnits.apply(i))
              ))
            )
          })
          .getOrElse(Nil)
      ).getOrElse(Nil)
  }

  /**
   * Get corresponding ATC codes for a RxNorm ingredient concept
   * See https://lhncbc.nlm.nih.gov/RxNav/APIs/api-RxNorm.getRxProperty.html
   * @param rxcui Concept id
   * @return
   */
  def getAtcCode(rxcui:String):Seq[String] = {
    val uri:String = s"$rxNormApiRootUrl/REST/rxcui/$rxcui/property.json?propName=ATC"
    val request =
      HttpRequest
        .apply(
          HttpMethods.GET,
          uri = Uri.apply(uri)
        )
    executeRequest(request)
      .flatMap(response =>
        FHIRUtil
          .extractValueOptionByPath[Seq[String]](response, "propConceptGroup.propConcept.propValue")
          .map(_.filter(r => r != "" && r != "/")))
      .getOrElse(Nil)
  }

  /**
   * Call the RxNorm Rest Service with JSON format and return the response JSON content, if success
   *
   * @param request
   * @return
   */
  private def executeRequest(request: HttpRequest): Option[Resource] = {
    implicit val actorSystem: ActorSystem = RxNormApiClient.actorSystem
    implicit val ec:ExecutionContext = actorSystem.dispatcher
    import RxNormApiClient.rxnormApiJsonUnmarshaller
    val call =
      Http()
        .singleRequest(request)
        .flatMap {
          case HttpResponse(StatusCodes.OK, _, entity, _) => Unmarshal.apply(entity).to[Option[Resource]]
          case HttpResponse(oth, _, entity, _) =>
            entity.discardBytes()
            throw new FhirPathException(s"Problem while accessing RxNorm API given with root URL $rxNormApiRootUrl! Rest call to ${request.uri.toString()} returns $oth!")
        }
    Await.result(call, Duration.apply(timeoutInSec, TimeUnit.SECONDS))
  }
}

object RxNormApiClient {
  implicit val actorSystem: ActorSystem = ActorSystem("RxNormApiClient")

  /**
   * Unmarshaller for
   */
  implicit val rxnormApiJsonUnmarshaller: Unmarshaller[HttpEntity, Option[Resource]] =
    Unmarshaller
      .stringUnmarshaller
      .forContentTypes(ContentTypes.`application/json`)
      .mapWithInput {
        case (entity: HttpEntity, data: String) =>
          if (entity.isKnownEmpty() || data.isEmpty) {
            None
          } else {
            val resource = data.parseJson
            if (resource.obj.isEmpty) None else Some(resource)
          }
      }
}
