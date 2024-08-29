package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, StatusCodes}
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.model.Project
import org.json4s.jackson.Serialization.writePretty


class ToFhirRejectionHandlerTest extends BaseEndpointTest {
  val project1: Project = Project(name = "example", description = Some("example project"))

  "ToFhirRejectionHandler" should {

    "create an HTTP response with unsupported media type status for invalid content type" in {
      // Send a POST request without changing the contentType to application/json
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}", akka.http.scaladsl.model.HttpEntity.apply(writePretty(project1))) ~> route ~> check {
        status shouldEqual StatusCodes.UnsupportedMediaType
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/UnsupportedMediaType")
      }
    }

    "create an HTTP response with bad request status for content not complying with the data model" in {
      // Send a POST request with a project without name
      val projectWithoutUrl = Map("url" -> "http://example3.com/example-project",
        "description" -> Some("example project"))
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(projectWithoutUrl))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/BadRequest")
        response should include("Detail: No usable value for name")
      }
    }

    "create an HTTP response with method not allowed status for requests initiated with an unsupported method" in {
      // Send a PUT request to projects which is not applicable
      Put(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(project1))) ~> route ~> check {
        status shouldEqual StatusCodes.MethodNotAllowed
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/MethodForbidden")
      }
    }

    "create an HTTP response with not found status for wrong URL" in {
      // Send a POST request to an invalid URL
      Post(s"/${webServerConfig.baseUri}/invalidURL", akka.http.scaladsl.model.HttpEntity.apply(ContentTypes.`application/json`, writePretty(project1))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/ResourceNotFound")
        response should include(s"/${webServerConfig.baseUri}/invalidURL does not exist")
      }
    }
  }
}
