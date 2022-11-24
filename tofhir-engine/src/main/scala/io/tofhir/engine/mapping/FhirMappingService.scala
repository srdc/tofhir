package io.tofhir.engine.mapping

import io.onfhir.api.Resource
import io.onfhir.expression.{FhirExpression, FhirExpressionException}
import io.onfhir.path.{FhirPathNavFunctionsFactory, FhirPathUtilFunctionsFactory}
import io.onfhir.template.FhirTemplateExpressionHandler
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.model.{ConfigurationContext, FhirInteraction, FhirMappingContext, FhirMappingException, FhirMappingExpression, IdentityServiceSettings, TerminologyServiceSettings}
import org.json4s.JsonAST.JArray
import org.json4s.{JObject, JValue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Mapping service for a specific FhirMapping together with contextual data and mapping scripts
 *
 * @param sources                     List of source aliases
 * @param context                     Context data for mappings
 * @param mappings                    Mapping scripts
 * @param terminologyServiceSettings  Settings for terminology service to use within mappings (e.g. lookupDisplay)
 * @param identityServiceSettings     Settings for identity service to use within mappings (e.g. resolveIdentifier)
 */
class FhirMappingService(
                          val jobId:String,
                          val mappingUrl:String,
                          val sources: Seq[String],
                          context: Map[String, FhirMappingContext],
                          mappings: Seq[FhirMappingExpression],
                          terminologyServiceSettings: Option[TerminologyServiceSettings],
                          identityServiceSettings: Option[IdentityServiceSettings]
                        ) extends IFhirMappingService {

  lazy val terminologyService = terminologyServiceSettings.map(setting => IntegratedServiceFactory.createTerminologyService(setting))
  lazy val identityService = identityServiceSettings.map(setting => IntegratedServiceFactory.createIdentityService(setting))

  /**
   * Template expression handler that will perform the mapping by executing the placeholder expressions
   */
  lazy val templateEngine =
    new FhirTemplateExpressionHandler(
      context.filter(_._2.isInstanceOf[ConfigurationContext]).map(c => c._1 -> c._2.toContextObject), // Provide the static contexts
      Map(
        "mpp" -> new FhirMappingFunctionsFactory(context.filterNot(_._2.isInstanceOf[ConfigurationContext])),
        FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory,
        FhirPathNavFunctionsFactory.defaultPrefix -> FhirPathNavFunctionsFactory
      ), //Add our mapping function library,
      terminologyService,
      identityService,
      isSourceContentFhir = false
    )

  /**
   * For single source mappings, map the given source into one or more FHIR resources based on the underlying mapping definition for this service
   *
   * @param source Input object
   * @return  List of converted resources for each mapping expression
   */
  override def mapToFhir(source: JObject): Future[Seq[(String, Seq[Resource], Option[FhirInteraction])]] = {
    //Find out eligible mappings on this source JObject based on preconditions
    val eligibleMappings =
      mappings
        .filter(mpp =>
          mpp
            .precondition
            .forall(prc => templateEngine.fhirPathEvaluator.satisfies(prc.expression.get, source))
        )

    //Execute the eligible mappings sequentially while appending previous mapping results as context parameter
    eligibleMappings
      .foldLeft[Future[(Map[String, JValue], Seq[(String,JValue,Option[FhirInteraction])])]](Future.apply(Map.empty[String, JValue] -> Seq.empty[(String,JValue,Option[FhirInteraction])])) {
        case (fresults, mpp) =>
          // Evaluate the expressions within the FHIR interaction if exists
          val fhirInteractionFuture =
            mpp
              .fhirInteraction match {
                case None => Future.apply(None)
                case Some(fhirIntr) if fhirIntr.condition.isDefined =>
                  evaluateExpressionReturnString(fhirIntr.condition.get, Map.empty[String, JValue], source)
                    .map(cnd =>
                      Some(fhirIntr.copy(condition = Some(cnd)))
                    )
                case Some(fhirIntr) if fhirIntr.rid.isDefined =>
                  evaluateExpressionReturnString(fhirIntr.rid.get, Map.empty[String, JValue], source)
                    .map(rid => Some(fhirIntr.copy(rid = Some(rid))))
              }
          //Evaluate each mapping expression
          fhirInteractionFuture
            .flatMap(fhirInteraction =>
              fresults
                .flatMap {
                  case (cntx, results) =>
                    templateEngine
                      .evaluateExpression(mpp.expression, cntx, source) //Evaluate the template expression
                      .map(r => // Get result of the evaluated expression
                        (cntx + (mpp.expression.name -> r)) -> //Append the new result to dynamic context params set
                          (results :+ (mpp.expression.name, r, fhirInteraction)) //Append the result to result set (resources are accumulating)
                      )
                      .recover {
                        case e: FhirExpressionException =>
                          throw FhirMappingException(mpp.expression.name, e)
                      }
                }
            )
      }
      .map(_._2) // Get the accumulated result set
      .map(resources =>
        resources.map {
          case (expName, a: JArray, fhirIntr) => (expName, a.arr.map(_.asInstanceOf[Resource]), fhirIntr)
          case (expName, o: JObject, fhirIntr) =>  (expName, Seq(o), fhirIntr)
          case _ => throw new IllegalStateException("This is an unexpected situation. Among the FHIR resources returned by evaluatedExpression, there is something which is neither JArray nor JObject.")
        }
      )
  }

  private def evaluateExpressionReturnString(expr:String, cntx:Map[String, JValue], input:JValue):Future[String] = {
      templateEngine
        .evaluateExpression(
          FhirExpression(name = "...", language = "application/fhir-template+json", expression = Some(expr), None, None),
          cntx,
          input
        ).map(_.extract[String])
  }

  /**
   * Map given source set into one or more FHIR resources based on the underlying mapping definition for this service
   *
   * @param sources Map of source data (alis of the source in mapping definition FhirMapping.source.alias) -> Source object(s) as the input to the mapping
   * @return
   */
  override def mapToFhir(sources: Map[String, Seq[JObject]]): Future[Seq[(String, Seq[Resource], Option[FhirInteraction])]] = ???
}
