package io.tofhir.test

import io.onfhir.path.FhirPathEvaluator
import io.tofhir.ToFhirTestSpec
import io.tofhir.engine.mapping.context.MappingContextLoader
import io.tofhir.engine.mapping.fhirPath.FhirMappingFunctionsFactory
import io.tofhir.engine.model.{FhirMapping, FhirMappingContextDefinition}
import org.json4s.jackson.JsonMethods
import org.json4s.{JArray, JNull, JObject, JString, JValue}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.must.Matchers.convertToAnyMustWrapper

import scala.io.Source

class FhirPathMappingFunctionsTest extends AsyncFlatSpec with ToFhirTestSpec {

  // a simple json on which we can run mpp:nonEmptyLoopedFields function
  val loopJson: JValue = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/test-data/loop.json")).mkString)

  val labResultMapping: FhirMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/lab-results-mapping")
  val conceptMapContextDefinition: FhirMappingContextDefinition = labResultMapping.context("obsConceptMap")
  val unitConversionContextDefinition: FhirMappingContextDefinition = labResultMapping.context("labResultUnitConversion")
  val mappingContextLoader = new MappingContextLoader

  "A FhirPathMappingsFunctions" should "correctly execute the getConcept function" in {
    mappingContextLoader.retrieveContext(conceptMapContextDefinition) map { mappingContext =>
      val fhirPathMappingFunctions = new FhirMappingFunctionsFactory(Map("obsConceptMap" -> mappingContext))

      val sourceSystem = FhirPathEvaluator().withFunctionLibrary("mpp", fhirPathMappingFunctions).evaluateOptionalString("mpp:getConcept(%obsConceptMap, '1299-7', 'source_system')", JNull)
      sourceSystem shouldBe Some("http://loinc.org")
      val sourceDisplay = FhirPathEvaluator().withFunctionLibrary("mpp", fhirPathMappingFunctions).evaluateOptionalString("mpp:getConcept(%obsConceptMap, '1299-7', 'source_display')", JNull)
      sourceDisplay shouldBe Some("Trom (cumulative) given")
      val unit = FhirPathEvaluator().withFunctionLibrary("mpp", fhirPathMappingFunctions).evaluateOptionalString("mpp:getConcept(%obsConceptMap, '1299-7', 'unit')", JNull)
      unit shouldBe Some("mL")
      val profile = FhirPathEvaluator().withFunctionLibrary("mpp", fhirPathMappingFunctions).evaluateOptionalString("mpp:getConcept(%obsConceptMap, '1299-7', 'profile')", JNull)
      profile shouldBe Some("https://aiccelerate.eu/fhir/StructureDefinition/AIC-IntraOperativeObservation")

      val unknownCode = FhirPathEvaluator().withFunctionLibrary("mpp", fhirPathMappingFunctions).evaluateOptionalString("mpp:getConcept(%obsConceptMap, 'UNKNOWN_CODE', 'profile')", JNull)
      unknownCode shouldBe None
      val unknownColumn = FhirPathEvaluator().withFunctionLibrary("mpp", fhirPathMappingFunctions).evaluateOptionalString("mpp:getConcept(%obsConceptMap, '1299-7', 'UNKNOWN_COLUMN')", JNull)
      unknownColumn shouldBe None
    }
  }

  it should "correctly execute the convertAndReturnQuantity function" in {
    mappingContextLoader.retrieveContext(unitConversionContextDefinition) map { mappingContext =>
      val fhirPathMappingFunctions = new FhirMappingFunctionsFactory(Map("labResultUnitConversion" -> mappingContext))
      // 1552,g/l,g/dL,"""$this * 0.1"""
      val valueQuantity = FhirPathEvaluator().withDefaultFunctionLibraries().withFunctionLibrary("mpp", fhirPathMappingFunctions)
        .evaluateAndReturnJson("mpp:convertAndReturnQuantity(%labResultUnitConversion, '1552', 100, 'g/l')", JNull)
      valueQuantity.isDefined shouldBe true
      valueQuantity.get shouldBe a[JObject]

      val obj = valueQuantity.get.asInstanceOf[JObject].values
      obj("value") shouldBe 10
      obj("system") shouldBe "http://unitsofmeasure.org"
      obj("unit") shouldBe "g/dL"
      obj("code") shouldBe "g/dL"

      val unknownConversion = FhirPathEvaluator().withDefaultFunctionLibraries().withFunctionLibrary("mpp", fhirPathMappingFunctions).evaluateOptionalString("mpp:convertAndReturnQuantity(%labResultUnitConversion, 'UNKNOWN_CODE', 100, 'UNKNOWN_UNIT')", JNull)
      unknownConversion shouldBe None
    }
  }

  it should "correctly execute getHashedId" in {
    val fhirEvaluator = FhirPathEvaluator().withFunctionLibrary("mpp",  new FhirMappingFunctionsFactory(Map.empty))
    val hash1 = fhirEvaluator.evaluateOptionalString("mpp:getHashedId('Patient','p1')", JNull)
    val hash2 = fhirEvaluator.evaluateOptionalString("mpp:getHashedId('Patient', 'p1')", JNull)
    hash1 shouldBe hash2
    val hash3 = fhirEvaluator.evaluateOptionalString("mpp:getHashedId('Patient','p2')", JNull)
    hash1 != hash3 shouldBe(true)
  }

  it should "correctly execute nonEmptyLoopedFields" in {
    val fhirEvaluator = FhirPathEvaluator().withFunctionLibrary("mpp",  new FhirMappingFunctionsFactory(Map.empty))
    val sct = fhirEvaluator.evaluateAndReturnJson("mpp:nonEmptyLoopedFields('sct_8116006_',1,5)", loopJson).head
    sct mustEqual JArray(List(JString("sct_8116006_1"), JString("sct_8116006_2")))
  }
}
