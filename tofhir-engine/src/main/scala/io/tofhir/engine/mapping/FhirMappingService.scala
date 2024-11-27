package io.tofhir.engine.mapping

import io.onfhir.api.Resource
import io.onfhir.expression.{FhirExpression, FhirExpressionException}
import io.onfhir.path.{FhirPathEvaluator, IFhirPathFunctionLibraryFactory}
import io.onfhir.template.FhirTemplateExpressionHandler
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.engine.mapping.fhirPath.FhirMappingFunctionsFactory
import io.tofhir.engine.mapping.service.IntegratedServiceFactory
import io.tofhir.engine.model._
import io.tofhir.engine.model.exception.FhirMappingException
import org.json4s.JsonAST.{JArray, JNull, JObject, JValue}

import io.tofhir.engine.Execution.actorSystem.dispatcher
import scala.concurrent.Future

/**
 * Mapping service for a specific FhirMapping together with contextual data and mapping scripts
 *
 * @param jobId                      Identifier of the job referring to the mapping
 * @param mappingTaskName            The name of the mapping task being executed
 * @param sources                    List of source aliases
 * @param context                    Context data for mappings
 * @param mappings                   Mapping scripts
 * @param variables                  Variables defined in the mapping
 * @param terminologyServiceSettings Settings for terminology service to use within mappings (e.g. lookupDisplay)
 * @param identityServiceSettings    Settings for identity service to use within mappings (e.g. resolveIdentifier)
 * @param functionLibraries          External function libraries containing functions to use in FHIRPath expressions
 * @param projectId                  Project identifier associated with the mapping job
 *                                   (if true, mapped FHIR resources are grouped by input row in the FhirMappingResult)
 */
class FhirMappingService(val jobId: String,
                         val mappingTaskName: String,
                         val sources: Seq[String],
                         context: Map[String, FhirMappingContext],
                         mappings: Seq[FhirMappingExpression],
                         variables: Seq[FhirExpression],
                         terminologyServiceSettings: Option[TerminologyServiceSettings],
                         identityServiceSettings: Option[IdentityServiceSettings],
                         functionLibraries: Map[String, IFhirPathFunctionLibraryFactory],
                         val projectId: Option[String]
                        ) extends IFhirMappingService {

  lazy val terminologyService = terminologyServiceSettings.map(setting => IntegratedServiceFactory.createTerminologyService(setting))
  lazy val identityService = identityServiceSettings.map(setting => IntegratedServiceFactory.createIdentityService(setting))

  /**
   * Template expression handler that will perform the mapping by executing the placeholder expressions
   */
  lazy val templateEngine =
    new FhirTemplateExpressionHandler(
      context.filter(_._2.isInstanceOf[ConfigurationContext]).map(c => c._1 -> c._2.toContextObject), // Provide the static contexts
      functionLibraries + // Default libraries
        ("mpp" -> new FhirMappingFunctionsFactory(context.filterNot(_._2.isInstanceOf[ConfigurationContext]))), //Add our mapping function library,
      terminologyService,
      identityService,
      isSourceContentFhir = false
    )

  /**
   * Map the given source into one or more FHIR resources based on the underlying mapping definition for this service
   *
   * @param source Input object
   * @return List of converted resources for each mapping expression
   */
  override def mapToFhir(source: JObject, otherSourceAsContextVariables: Map[String, JValue] = Map.empty): Future[Seq[(String, Seq[Resource], Option[FhirInteraction])]] = {
    //Calculate the variables
    val contextVariables: Map[String, JValue] =
      variables.foldLeft(otherSourceAsContextVariables) {
        case (context, vexp) =>
          evaluateFhirPathExpression(vexp, source, context) match {
            case Some(vl) =>
              context + (vexp.name -> vl)
            case None =>
              context
          }
      }

    //Find out eligible mappings on this source JObject based on preconditions
    val eligibleMappings: Seq[FhirMappingExpression] =
      mappings
        .filter(mpp =>
          mpp
            .precondition
            .forall(prc =>
              try {
                getFhirPathEvaluator(contextVariables)
                  .evaluateOptionalBoolean(prc.expression.get, source)
                  .exists(t => t)
              } catch {
                case e: Exception =>
                  throw FhirMappingException(s"Expression: ${mpp.expression.name}. Error: ${e.getMessage}", e)
              }
            )
        )

    //Execute the eligible mappings sequentially while appending previous mapping results as context parameter
    eligibleMappings
      .foldLeft[Future[(Map[String, JValue], Seq[(String, JValue, Option[FhirInteraction])])]](Future.apply(contextVariables -> Seq.empty[(String, JValue, Option[FhirInteraction])])) {
        case (fresults, mpp) =>
          // Evaluate the expressions within the FHIR interaction if exists
          val fhirInteractionFuture = mpp.fhirInteraction match {
            case None => Future.apply(None)
            case Some(fhirIntr) if fhirIntr.condition.isDefined =>
              evaluateExpressionReturnString(fhirIntr.condition.get, contextVariables, source)
                .map(cnd => Some(fhirIntr.copy(condition = Some(cnd))))
                .recover {
                  case e: Exception =>
                    throw FhirMappingException(s"Expression: ${mpp.expression.name}. Error: ${e.getMessage}", e)
                }
            case Some(fhirIntr) if fhirIntr.rid.isDefined =>
              evaluateExpressionReturnString(fhirIntr.rid.get, contextVariables, source)
                .map(rid => Some(fhirIntr.copy(rid = Some(rid))))
                .recover {
                  case e: Exception =>
                    throw FhirMappingException(s"Expression: ${mpp.expression.name}. Error: ${e.getMessage}", e)
                }
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
                          throw FhirMappingException(s"Expression: ${mpp.expression.name}. Error: ${e.getMessage}", e)
                      }
                }
            )
      }
      .map(_._2) // Get the accumulated result set
      .map(resources =>
        resources.map {
          case (expName, JNull, fhirIntr) => (expName, Nil, fhirIntr)
          case (expName, a: JArray, fhirIntr) =>
            // If we use section template inside a section template (we use that to achieve loop in a loop),
            // we want to flatten the returned Resources.
            val flattenedResources = flattenJArrayRecursively(a)
            (expName, flattenedResources, fhirIntr)
          case (expName, o: JObject, fhirIntr) => (expName, Seq(o), fhirIntr)
          case _ => throw new IllegalStateException("This is an unexpected situation. Among the FHIR resources returned by evaluatedExpression, there is something which is neither JArray nor JObject.")
        }
      )
  }

  /**
   * Given a JArray, its inner elements can also be of type JArray. If that is the case,
   * * this function recursively traverses all JArrays and prepares a flattened list of
   * * JObjects (Resources).
   *
   * @param inputArray
   * @return
   */
  private def flattenJArrayRecursively(inputArray: JArray): Seq[Resource] = {
    def flatten(arr: List[JValue]): Seq[JObject] = arr match {
      case Nil => Seq.empty
      case (head: JArray) :: tail =>
        // Recursively flatten JArray elements
        flatten(head.arr) ++ flatten(tail)
      case (head: JObject) :: tail =>
        // Collect JObject elements
        head +: flatten(tail)
      case _ :: tail =>
        // Ignore non-JObject or non-JArray elements
        flatten(tail)
    }
    flatten(inputArray.arr)
  }

  /**
   * Evaluate the FHIR Path expression
   *
   * @param fhirExpression FHIR Path expression
   * @param source         Input content
   * @return
   */
  private def evaluateFhirPathExpression(fhirExpression: FhirExpression, source: JObject, context: Map[String, JValue]): Option[JValue] = {
    try {
      getFhirPathEvaluator(context)
        .evaluateAndReturnJson(fhirExpression.expression.get, source)
    } catch {
      case e: Exception =>
        throw FhirMappingException(s"Expression: ${fhirExpression.name}. Error: ${e.getMessage}", e)
    }
  }

  /**
   * Return the FhirPathEvaluator with the supplied parameters
   *
   * @param context
   * @return
   */
  private def getFhirPathEvaluator(context: Map[String, JValue]): FhirPathEvaluator = {
    context.foldLeft(templateEngine.fhirPathEvaluator) {
      case (evaluator, v) => evaluator.withEnvironmentVariable(v._1, v._2)
    }
  }

  /**
   * Evaluate the expression and return string
   *
   * @param expr  Template expression
   * @param cntx  FHIR Path context
   * @param input Input content
   * @return
   */
  private def evaluateExpressionReturnString(expr: String, cntx: Map[String, JValue], input: JValue): Future[String] = {
    templateEngine
      .evaluateExpression(
        FhirExpression(name = "...", language = "application/fhir-template+json", expression = Some(expr)),
        cntx,
        input
      ).map(_.extract[String])

  }
}
