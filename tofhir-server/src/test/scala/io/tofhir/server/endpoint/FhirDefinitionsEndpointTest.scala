package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import io.onfhir.definitions.resource.endpoint.FhirDefinitionsEndpoint
import io.onfhir.definitions.resource.endpoint.FhirDefinitionsEndpoint.{DefinitionsQuery, QUERY_PARAM_PROFILE, QUERY_PARAM_Q}
import io.tofhir.OnFhirTestContainer
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.onfhir.definitions.common.model.{SchemaDefinition, SimpleStructureDefinition}
import io.tofhir.engine.util.MajorFhirVersion
import io.tofhir.server.BaseEndpointTest
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

  /**
   * Creates a project to be used in the tests.
   * */
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.createProject()
  }

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

    /**
     * Test suite for retrieving base FHIR resources via the FHIR Definitions endpoint.
     */
    "retrieve base FHIR resources" in {
      /**
       * Test case to check if a BadRequest status is returned when the query parameter 'fhirVersion' is missing.
       */
      Get(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_BASE_PROFILES}") ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] should include("Detail: base-profiles path cannot be invoked without the query parameter 'fhirVersion'.")
      }

      /**
       * Test case to check if the correct base FHIR resource types are returned for FHIR version R4.
       */
      Get(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_BASE_PROFILES}?${FhirDefinitionsEndpoint.QUERY_PARAM_FHIR_VERSION}=${MajorFhirVersion.R4}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        JsonMethods.parse(responseAs[String]).extract[List[String]].length shouldEqual 147
      }

      /**
       * Test case to check if a BadRequest status is returned when an invalid FHIR version is provided.
       */
      Get(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_BASE_PROFILES}?${FhirDefinitionsEndpoint.QUERY_PARAM_FHIR_VERSION}=R6") ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] should include("Detail: fhirVersion on base-profiles cannot take the value: R6. Possible values are: R4 and R5")
      }

      /**
       * Test case to check if the correct schema definition is returned for a single profile URL with FHIR version R4.
       */
      Get(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_BASE_PROFILES}?${FhirDefinitionsEndpoint.QUERY_PARAM_FHIR_VERSION}=${MajorFhirVersion.R4}&${FhirDefinitionsEndpoint.QUERY_PARAM_PROFILE}=http://hl7.org/fhir/StructureDefinition/Patient") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val responseBody = JsonMethods.parse(responseAs[String]).extract[Seq[SchemaDefinition]]
        responseBody.length shouldEqual 1
        responseBody.head.url shouldEqual "http://hl7.org/fhir/StructureDefinition/Patient"
        responseBody.head.id shouldEqual "Patient"
        responseBody.head.fieldDefinitions.get.length shouldEqual 22
      }

      /**
       * Test case to check if the correct schema definitions are returned for multiple profile URLs with FHIR version R4.
       */
      Get(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_BASE_PROFILES}?${FhirDefinitionsEndpoint.QUERY_PARAM_FHIR_VERSION}=${MajorFhirVersion.R4}&${FhirDefinitionsEndpoint.QUERY_PARAM_PROFILE}=http://hl7.org/fhir/StructureDefinition/Patient,http://hl7.org/fhir/StructureDefinition/Condition") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val responseBody = JsonMethods.parse(responseAs[String]).extract[Seq[SchemaDefinition]]
        responseBody.length shouldEqual 2
        responseBody.head.url shouldEqual "http://hl7.org/fhir/StructureDefinition/Condition"
        responseBody.head.id shouldEqual "Condition"
        responseBody.head.fieldDefinitions.get.length shouldEqual 23
        responseBody.last.url shouldEqual "http://hl7.org/fhir/StructureDefinition/Patient"
        responseBody.last.id shouldEqual "Patient"
        responseBody.last.fieldDefinitions.get.length shouldEqual 22
      }
    }

    /**
     * Test case to verify retrieval of simplified element definitions for a given FHIR profile.
     * */
    "retrieve simplified element definitions of a FHIR profile" in {
      Get(s"/${webServerConfig.baseUri}/${FhirDefinitionsEndpoint.SEGMENT_FHIR_DEFINITIONS}?$QUERY_PARAM_Q=${DefinitionsQuery.ELEMENTS}&$QUERY_PARAM_PROFILE=http://hl7.org/fhir/StructureDefinition/Patient") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        JsonMethods.parse(responseAs[String]).extract[Seq[SimpleStructureDefinition]].length shouldEqual 22
      }
    }
  }
}

