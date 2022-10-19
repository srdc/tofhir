package io.tofhir.server.common.endpoint

import akka.http.scaladsl.marshalling.ToResponseMarshaller
import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.Route
import io.tofhir.server.common.model.{IToFhirRequest, IToFhirResponse, ToFhirRestCall}

import scala.concurrent.{ExecutionContext, Future}

/**
 * Interface for any toFHIR Endpoint
 */
trait IToFhirEndpoint {

  /**
   * Handle a toFHIR REST call (common authorization and other task handling)
   *
   * @param requestWrapper     Wrapper object for the REST call
   * @param request            Specific request object
   * @param requestHandler     Request handler function
   * @param responseMarshaller JSON Marshaller for the response
   * @param executionContext   Execution context
   * @tparam RQ  Request type
   * @tparam RSP Response type
   * @return
   */
  def handleRequest[RQ <: IToFhirRequest, RSP <: IToFhirResponse](
                                                                   requestWrapper: ToFhirRestCall,
                                                                   request: RQ,
                                                                   requestHandler: (RQ => Future[RSP])
                                                                   //authorizationHandler:BaseAuthzManager[_>:RQ]
                                                                 )(implicit responseMarshaller: ToResponseMarshaller[RSP], executionContext: ExecutionContext): Route = {

    def completeRequest(finalRequest: RQ) = {
      complete(
        requestHandler(finalRequest)
          .map(resp => {
            requestWrapper.response = Some(resp)
            resp
          })
      )
    }

    //TODO add authorization logic
    completeRequest(request)
  }
}
