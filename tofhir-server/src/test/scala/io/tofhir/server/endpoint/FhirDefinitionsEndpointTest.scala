package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import io.tofhir.OnFhirTestContainer
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.endpoint.FhirDefinitionsEndpoint
import org.json4s.JsonAST.{JString, JValue}
import org.json4s._
import org.json4s.jackson.JsonMethods

import scala.io.Source

/**
 * Test class for the FhirDefinitionsEndpoint.
 */
class FhirDefinitionsEndpointTest extends BaseEndpointTest with OnFhirTestContainer {

  // Bundle with three patients: 1 is valid, 1 has an invalid birthdate, and 1 has an invalid active field
  val patientBundleJson: String = Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/patient-bundle.json")).mkString
  // Valid condition resource
  val conditionResourceJson: String = Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/condition-resource.json")).mkString
  // Condition resource with missing subject
  val invalidConditionResourceJson: String = Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/invalid-condition-resource.json")).mkString

  "Fhir definitions endpoint" should {

    /**
     * Test case for validating FHIR resources.
     */
    "validate FHIR resources" in {

      // Validate the resource without providing a FHIR validation URL
      Post(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_VALIDATE}", HttpEntity(ContentTypes.`application/json`, conditionResourceJson)) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
      }

      // Validate the resource with an invalid FHIR validation URL
      Post(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_VALIDATE}?${FhirDefinitionsEndpoint.QUERY_PARAM_FHIRVALIDATIONURL}=test-url", HttpEntity(ContentTypes.`application/json`, conditionResourceJson)) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
      }

      // Validate the resource with a valid FHIR validation URL
      Post(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_VALIDATE}?${FhirDefinitionsEndpoint.QUERY_PARAM_FHIRVALIDATIONURL}=${this.onFhirClient.getBaseUrl()}/Condition/$$validate", HttpEntity(ContentTypes.`application/json`, conditionResourceJson)) ~> route ~> check {
        status shouldEqual StatusCodes.OK
      }

      // Validate the resource with a valid FHIR validation URL
      Post(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_VALIDATE}?${FhirDefinitionsEndpoint.QUERY_PARAM_FHIRVALIDATIONURL}=${this.onFhirClient.getBaseUrl()}/Condition/$$validate", HttpEntity(ContentTypes.`application/json`, invalidConditionResourceJson)) ~> route ~> check {
        status shouldEqual StatusCodes.OK

        // Convert the JSON response to a JValue
        val jsonResponse: JValue = JsonMethods.parse(responseAs[String])

        // Check the outcome issues
        val errorIssue = (jsonResponse \ "issue").asInstanceOf[JArray].arr.lift(1).get
        (errorIssue \ "severity") should be(JString("error"))
        (errorIssue \ "code") should be(JString("invalid"))
        (errorIssue \ "diagnostics") should be(JString("[Validating against 'http://hl7.org/fhir/StructureDefinition/Condition'] => Element 'subject' with data type(s) 'Reference' is required , but does not exist!"))
        (errorIssue \ "expression").asInstanceOf[JArray].arr.head should be(JString("subject"))
      }

      // Validate a bundle of resources
      Post(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_VALIDATE}?${FhirDefinitionsEndpoint.QUERY_PARAM_FHIRVALIDATIONURL}=${this.onFhirClient.getBaseUrl()}", HttpEntity(ContentTypes.`application/json`, patientBundleJson)) ~> route ~> check {
        status shouldEqual StatusCodes.OK

        // Convert the JSON response to a JValue
        val jsonResponse: JValue = JsonMethods.parse(responseAs[String])

        // Check the outcome issues for each entry
        val entries = (jsonResponse \ "entry").asInstanceOf[JArray]
        entries.arr.size should be(3)

        val firstEntryFirstIssue = (entries.arr.head \ "resource" \ "issue").asInstanceOf[JArray].arr.head
        (firstEntryFirstIssue \ "severity") should be(JString("information"))
        (firstEntryFirstIssue \ "code") should be(JString("informational"))
        (firstEntryFirstIssue \ "diagnostics") should be(JString("All OK :)"))
        (firstEntryFirstIssue \ "expression").asInstanceOf[JArray].arr.isEmpty should be(true)

        val secondEntryFirstIssue = (entries.arr.lift(1).get \ "resource" \ "issue").asInstanceOf[JArray].arr.head
        (secondEntryFirstIssue \ "severity") should be(JString("error"))
        (secondEntryFirstIssue \ "code") should be(JString("invalid"))
        (secondEntryFirstIssue \ "diagnostics").asInstanceOf[JString].s.split(" => ").last should be("Invalid value '1' for FHIR primitive type 'boolean'!")
        (secondEntryFirstIssue \ "expression").asInstanceOf[JArray].arr.head should be(JString("active"))

        val thirdEntryFirstIssue = (entries.arr.last \ "resource" \ "issue").asInstanceOf[JArray].arr.head
        (thirdEntryFirstIssue \ "severity") should be(JString("error"))
        (thirdEntryFirstIssue \ "code") should be(JString("invalid"))
        (thirdEntryFirstIssue \ "diagnostics").asInstanceOf[JString].s.split(" => ").last should be("Invalid value 'test' for FHIR primitive type 'date'!")
        (thirdEntryFirstIssue \ "expression").asInstanceOf[JArray].arr.head should be(JString("birthDate"))
      }
    }
  }
}

