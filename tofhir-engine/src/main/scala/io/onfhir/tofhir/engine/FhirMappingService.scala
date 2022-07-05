package io.onfhir.tofhir.engine

import io.onfhir.api.Resource
import io.onfhir.expression.FhirExpressionException
import io.onfhir.path.{FhirPathNavFunctionsFactory, FhirPathUtilFunctionsFactory}
import io.onfhir.template.FhirTemplateExpressionHandler
import io.onfhir.tofhir.model.{ConfigurationContext, FhirMappingContext, FhirMappingException, FhirMappingExpression}
import io.onfhir.util.JsonFormatter._
import org.json4s.JsonAST.JArray
import org.json4s.jackson.Serialization
import org.json4s.{JObject, JValue}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
 * Mapping service for a specific FhirMapping together with contextual data and mapping scripts
 *
 * @param sources  List of source aliases
 * @param context  Context data
 * @param mappings Mapping scripts
 */
class FhirMappingService(
                          val sources: Seq[String],
                          context: Map[String, FhirMappingContext],
                          mappings: Seq[FhirMappingExpression]
                        ) extends IFhirMappingService {

  /**
   * Template expression handler that will perform the mapping by executing the placeholder expressions
   */
  val templateEngine =
    new FhirTemplateExpressionHandler(
      context.filter(_._2.isInstanceOf[ConfigurationContext]).map(c => c._1 -> c._2.toContextObject), // Provide the static contexts
      Map(
        "mpp" -> new FhirMappingFunctionsFactory(context.filterNot(_._2.isInstanceOf[ConfigurationContext])),
        FhirPathUtilFunctionsFactory.defaultPrefix -> FhirPathUtilFunctionsFactory,
        FhirPathNavFunctionsFactory.defaultPrefix -> FhirPathNavFunctionsFactory
      ) //Add our mapping function library
    )

  /**
   * For single source mappings, map the given source into one or more FHIR resources based on the underlying mapping definition for this service
   *
   * @param source
   * @return
   */
  override def mapToFhir(source: JObject): Future[Seq[Resource]] = {
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
      .foldLeft[Future[(Map[String, JValue], Seq[JValue])]](Future.apply(Map.empty[String, JValue] -> Seq.empty[JValue])) {
        case (fresults, mpp) =>
          fresults
            .flatMap {
              case (cntx, results) =>
                templateEngine
                  .evaluateExpression(mpp.expression, cntx, source) //Evaluate the template expression
                  .map(r => // Get result of the evaluated expression
                    (cntx + (mpp.expression.name -> r)) -> //Append the new result to dynamic context params set
                      (results :+ r) //Append the result to result set (resources are accumulating)
                  )
                  .recover {
                    case e: FhirExpressionException =>
                      val msg = s"Error while evaluating the mapping expression.\n Expression: ${Serialization.write(mpp.expression)}\n Source: ${Serialization.write(source)}\n"
                      throw FhirMappingException(msg, e)
                  }
            }
      }
      .map(_._2) // Get the accumulated result set
      .map(resources =>
        resources.flatMap {
          case a: JArray => a.arr.map(_.asInstanceOf[Resource])
          case o: JObject => Seq(o)
          case _ => throw new IllegalStateException("This is an unexpected situation. Among the FHIR resources returned by evaluatedExpression, there is something which is neither JArray nor JObject.")
        }
      )
  }

  /**
   * Map given source set into one or more FHIR resources based on the underlying mapping definition for this service
   *
   * @param sources Map of source data (alis of the source in mapping definition FhirMapping.source.alias) -> Source object(s) as the input to the mapping
   * @return
   */
  override def mapToFhir(sources: Map[String, Seq[JObject]]): Future[Seq[Resource]] = ???
}
