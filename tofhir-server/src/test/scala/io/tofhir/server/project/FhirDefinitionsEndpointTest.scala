package io.tofhir.server.project

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import io.onfhir.api.Resource
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.BaseEndpointTest
import org.json4s.JsonAST.{JString, JValue}
import org.json4s._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

import scala.io.Source

/**
 * Test class for the FhirDefinitionsEndpoint.
 */
class FhirDefinitionsEndpointTest extends BaseEndpointTest {

  // Bundle with three patients: 1 is valid, 1 has an invalid birthdate, and 1 has an invalid active field
  val patientBundleJson: Resource = Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/patient-bundle.json")).mkString.parseJson
  // Valid condition resource
  val conditionResourceJson: Resource = Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/condition-resource.json")).mkString.parseJson
  // Condition resource with missing subject
  val invalidConditionResourceJson: Resource = Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/invalid-condition-resource.json")).mkString.parseJson

  "The endpoint" should {

    /**
     * Test case for validating FHIR resources.
     */
    "validate FHIR resources" in {
      assume(fhirServerIsAvailable)

      // Validate the resource without providing a FHIR validation URL
      Post(s"/${webServerConfig.baseUri}/validate", HttpEntity(ContentTypes.`application/json`, writePretty(conditionResourceJson))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
      }

      // Validate the resource with an invalid FHIR validation URL
      Post(s"/${webServerConfig.baseUri}/validate?fhirValidationUrl=test-url", HttpEntity(ContentTypes.`application/json`, writePretty(conditionResourceJson))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
      }

      // Validate the resource with a valid FHIR validation URL
      Post(s"/${webServerConfig.baseUri}/validate?fhirValidationUrl=$fhirRepoUrl/Condition/$$validate", HttpEntity(ContentTypes.`application/json`, writePretty(conditionResourceJson))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
      }

      // Validate the resource with a valid FHIR validation URL
      Post(s"/${webServerConfig.baseUri}/validate?fhirValidationUrl=$fhirRepoUrl/Condition/$$validate", HttpEntity(ContentTypes.`application/json`, writePretty(invalidConditionResourceJson))) ~> route ~> check {
        status shouldEqual StatusCodes.OK

        // Convert the JSON response to a JValue
        val jsonResponse: JValue = JsonMethods.parse(responseAs[String])

        // Check the outcome issues
        val firstIssue = (jsonResponse \ "issue").asInstanceOf[JArray].arr.head
        (firstIssue \ "severity") should be(JString("error"))
        (firstIssue \ "code") should be(JString("invalid"))
        (firstIssue \ "diagnostics") should be(JString("[Validating against 'http://hl7.org/fhir/StructureDefinition/Condition'] => Element 'subject' with data type(s) 'Reference' is required , but does not exist!"))
        (firstIssue \ "expression").asInstanceOf[JArray].arr.head should be(JString("subject"))
      }

      // Validate a bundle of resources
      Post(s"/${webServerConfig.baseUri}/validate?fhirValidationUrl=$fhirRepoUrl", HttpEntity(ContentTypes.`application/json`, writePretty(patientBundleJson))) ~> route ~> check {
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

