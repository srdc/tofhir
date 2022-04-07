package io.onfhir.tofhir.engine

import io.onfhir.path.{FhirPathEvaluator, FhirPathException}
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.model.{ConceptMapContext, FhirMappingException}
import org.json4s.JsonAST.JLong
import org.json4s.{JNull, JObject, JString}

class FhirPathMappingFunctionsTest extends ToFhirTestSpec {

  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(getClass.getResource("/test-mappings-2").toURI)
  val labResultMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/lab-results-mapping")
  val conceptMapContextDefinition = labResultMapping.context("obsConceptMap")
  val unitConversionContextDefinition = labResultMapping.context("labResultUnitConversion")
  val mappingContextLoader = new MappingContextLoader(mappingRepository)

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

      val thrownException1 = the[FhirPathException] thrownBy
        FhirPathEvaluator().withFunctionLibrary("mpp", fhirPathMappingFunctions).evaluateOptionalString("mpp:getConcept(%obsConceptMap, 'UNKNOWN_CODE', 'profile')", JNull)
      thrownException1.getCause shouldBe a[FhirMappingException]
      thrownException1.getCause should have message "Concept code:UNKNOWN_CODE cannot be found in the ConceptMapContext:obsConceptMap"

      val thrownException2 = the[FhirPathException] thrownBy
        FhirPathEvaluator().withFunctionLibrary("mpp", fhirPathMappingFunctions).evaluateOptionalString("mpp:getConcept(%obsConceptMap, '1299-7', 'UNKNOWN_COLUMN')", JNull)
      thrownException2.getCause shouldBe a[FhirMappingException]
      thrownException2.getCause should have message s"For the given concept code:1299-7, the column:UNKNOWN_COLUMN cannot be " +
        s"found in the ConceptMapContext:obsConceptMap. Available columns are ${mappingContext.asInstanceOf[ConceptMapContext].concepts.head._2.keySet.mkString(",")}"
    }
  }

  it should "correctly execute the convertAndReturnQuantity function" in {
    mappingContextLoader.retrieveContext(unitConversionContextDefinition) map { mappingContext =>
      val fhirPathMappingFunctions = new FhirMappingFunctionsFactory(Map("labResultUnitConversion" -> mappingContext))
      // 1552,g/l,g/dL,"""$this * 0.1"""
      val valueQuantity = FhirPathEvaluator().withFunctionLibrary("mpp", fhirPathMappingFunctions)
        .evaluateAndReturnJson("mpp:convertAndReturnQuantity(%labResultUnitConversion, '1552', 100, 'g/l')", JNull)
      valueQuantity.isDefined shouldBe true
      valueQuantity.get shouldBe a[JObject]

      val obj = valueQuantity.get.asInstanceOf[JObject].values
      obj("value") shouldBe 10
      obj("system") shouldBe "http://unitsofmeasure.org"
      obj("unit") shouldBe "g/dL"
      obj("code") shouldBe "1552"

      val thrownException = the[FhirPathException] thrownBy
        FhirPathEvaluator().withFunctionLibrary("mpp", fhirPathMappingFunctions).evaluateOptionalString("mpp:convertAndReturnQuantity(%labResultUnitConversion, 'UNKNOWN_CODE', 100, 'UNKNOWN_UNIT')", JNull)
      thrownException.getCause shouldBe a[FhirMappingException]
      thrownException.getCause should have message "(code, unit) pair:(UNKNOWN_CODE, UNKNOWN_UNIT) cannot be found in the UnitConversionFunctionsContext:labResultUnitConversion"
    }
  }

}
