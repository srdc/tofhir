package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.onfhir.api.Resource
import io.tofhir.server.common.model.{BadRequest, ToFhirRestCall}
import io.tofhir.server.endpoint.FhirDefinitionsEndpoint.{DefinitionsQuery, QUERY_PARAM_FHIRVALIDATIONURL, QUERY_PARAM_FHIR_VERSION, QUERY_PARAM_PROFILE, QUERY_PARAM_Q, QUERY_PARAM_RTYPE, SEGMENT_BASE_PROFILES, SEGMENT_FHIR_DEFINITIONS, SEGMENT_VALIDATE}
import io.tofhir.server.fhir.FhirDefinitionsConfig
import io.tofhir.common.model.Json4sSupport._
import io.tofhir.engine.util.MajorFhirVersion
import io.tofhir.server.service.fhir.FhirDefinitionsService
import io.tofhir.server.service.fhir.base.FhirBaseProfilesService

class FhirDefinitionsEndpoint(fhirDefinitionsConfig: FhirDefinitionsConfig) extends LazyLogging {

  val service: FhirDefinitionsService = new FhirDefinitionsService(fhirDefinitionsConfig)

  def route(request: ToFhirRestCall): Route =
    pathPrefix(SEGMENT_FHIR_DEFINITIONS) {
      pathEndOrSingleSlash {
        get {
          parameterMap { queryParams =>
            queryParams.get(QUERY_PARAM_Q) match {
              case Some(v) =>
                DefinitionsQuery.withName(v) match {
                  case DefinitionsQuery.RESOURCE_TYPES => complete(service.getResourceTypes())
                  case DefinitionsQuery.PROFILES =>
                    queryParams.get(QUERY_PARAM_RTYPE) match {
                      case Some(rtype) => complete(service.getProfilesFor(rtype))
                      case None => throw BadRequest("Missing query parameter.", s"$SEGMENT_FHIR_DEFINITIONS?$QUERY_PARAM_Q=${DefinitionsQuery.PROFILES} cannot be invoked without the query parameter '$QUERY_PARAM_RTYPE'.")
                    }
                  case DefinitionsQuery.ELEMENTS =>
                    queryParams.get(QUERY_PARAM_PROFILE) match {
                      case Some(profileUrl) => complete(service.getElementDefinitionsOfProfile(profileUrl))
                      case None => complete(HttpResponse(StatusCodes.BadRequest)) // FIXME
                    }
                  case unk => throw BadRequest("Invalid parameter value.", s"$QUERY_PARAM_Q on $SEGMENT_FHIR_DEFINITIONS cannot take the value:$unk. Possible values are: ${DefinitionsQuery.values.mkString}")
                }
              case None => throw BadRequest("Missing query parameter.", s"$SEGMENT_FHIR_DEFINITIONS path cannot be invoked without the query parameter '$QUERY_PARAM_Q'.")
            }
          }
        }
      }
    } ~ pathPrefix(SEGMENT_VALIDATE) {
        pathEndOrSingleSlash {
          validateResource()
        }
      } ~ pathPrefix(SEGMENT_BASE_PROFILES) {
      pathEndOrSingleSlash {
        get {
          parameterMap { queryParams =>
            // FHIR version
            val fhirVersion = queryParams.get(QUERY_PARAM_FHIR_VERSION)
            // comma separated profile urls
            val profile = queryParams.get(QUERY_PARAM_PROFILE)

            (fhirVersion, profile) match {
              // return the SchemaDefinitions for the requested profiles
              case (Some(v), Some(p)) =>
                v match {
                  case MajorFhirVersion.R4 | MajorFhirVersion.R5 => complete(FhirBaseProfilesService.apply(v).getProfile(p))
                  case unk => throw BadRequest("Invalid parameter value.", s"$QUERY_PARAM_FHIR_VERSION on $SEGMENT_BASE_PROFILES cannot take the value: $unk. Possible values are: ${MajorFhirVersion.R4} and ${MajorFhirVersion.R5}")
                }
              // return the list of available resource types
              case (Some(v), None) =>
                v match {
                  case MajorFhirVersion.R4 | MajorFhirVersion.R5 => complete(FhirBaseProfilesService.apply(v).resourceTypes)
                  case unk => throw BadRequest("Invalid parameter value.", s"$QUERY_PARAM_FHIR_VERSION on $SEGMENT_BASE_PROFILES cannot take the value: $unk. Possible values are: ${MajorFhirVersion.R4} and ${MajorFhirVersion.R5}")
                }
              case (None, _) => throw BadRequest("Missing query parameter.", s"$SEGMENT_BASE_PROFILES path cannot be invoked without the query parameter '$QUERY_PARAM_FHIR_VERSION'.")
            }
          }
        }
      }
    }

  /**
   * Validates a FHIR resource against a given FHIR validation URL in 'fhirValidationUrl' query param.
   * @return
   */
  private def validateResource(): Route = {
    post {
      parameterMap { paramMap =>
        paramMap.get(QUERY_PARAM_FHIRVALIDATIONURL) match {
          case Some(fhirValidationUrl) =>
            entity(as[Resource]) { requestBody =>
              // call the API and return the response
              onComplete(service.validateResource(requestBody, fhirValidationUrl)) {
                case scala.util.Success(response) =>
                  complete(response)
                case scala.util.Failure(ex) =>
                  complete(StatusCodes.InternalServerError, s"Proxy request failed: ${ex.getMessage}")
              }
            }
          case None => throw BadRequest("Missing query parameter.", s"$SEGMENT_VALIDATE path cannot be invoked without the query parameter 'fhirValidationUrl'.")
        }
      }
    }
  }


}

object FhirDefinitionsEndpoint {
  val SEGMENT_FHIR_DEFINITIONS = "fhir-definitions"
  val SEGMENT_VALIDATE = "validate"
  val SEGMENT_BASE_PROFILES = "base-profiles"
  val QUERY_PARAM_Q = "q"
  val QUERY_PARAM_RTYPE = "rtype"
  val QUERY_PARAM_PROFILE = "profile"
  val QUERY_PARAM_FHIRVALIDATIONURL = "fhirValidationUrl"
  val QUERY_PARAM_FHIR_VERSION = "fhirVersion"

  object DefinitionsQuery extends Enumeration {
    type DefinitionsQuery = Value
    final val RESOURCE_TYPES = Value("rtypes")
    final val PROFILES = Value("profiles")
    final val ELEMENTS = Value("elements")
  }

}
