package io.tofhir.test

import io.tofhir.ToFhirTestSpec
import io.tofhir.engine.mapping.context.MappingContextLoader
import io.tofhir.engine.model.exception.FhirMappingException
import io.tofhir.engine.model.{ConceptMapContext, UnitConversionContext}
import org.scalatest.flatspec.AsyncFlatSpec

import java.io.File

class FhirMappingFolderRepositoryTest extends AsyncFlatSpec with ToFhirTestSpec {

  "A FhirMappingRepository" should "correctly read and parse the mapping files under the given mappings folder" in {
    val patientMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/patient-mapping")
    patientMapping.name shouldBe "patient-mapping"
    patientMapping.mapping.length shouldBe 1
    patientMapping.mapping.head.expression.value.isEmpty shouldBe false

    val observationMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/other-observation-mapping")
    observationMapping.name shouldBe "other-observation-mapping"
    observationMapping.mapping.length shouldBe 3
    observationMapping.mapping.head.expression.value.isEmpty shouldBe false

    val labResultsMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/lab-results-mapping")
    labResultsMapping.name shouldBe "lab-results-mapping"
    labResultsMapping.mapping.length shouldBe 1
    labResultsMapping.mapping.head.expression.value.isEmpty shouldBe false
  }

  it should "correctly arrange the relative paths of the context definitions" in {
    val observationMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/other-observation-mapping")
    val contextDefinition = observationMapping.context("obsConceptMap")
    contextDefinition.url.isDefined shouldBe true
    val contextFile = new File(contextDefinition.url.get)
    contextFile.exists() shouldBe true
  }

  it should "throw exception when an unknown mapping is requested" in {
    the[FhirMappingException] thrownBy mappingRepository.getFhirMappingByUrl("some-unknown-url") should have message s"FhirMapping with url some-unknown-url cannot be found in folder $repositoryFolderUri"
  }

  it should "correctly load concept map context definitions" in {
    val observationMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/other-observation-mapping")
    val contextDefinition = observationMapping.context("obsConceptMap")
    val mappingContextLoader = new MappingContextLoader
    mappingContextLoader.retrieveContext(contextDefinition) map { context =>
      val conceptMapContext = context.asInstanceOf[ConceptMapContext]
      conceptMapContext.concepts.size shouldBe 14

      // source_code,source_system,source_display,unit,profile
      // 9187-6,http://loinc.org,Urine Output,cm3,https://aiccelerate.eu/fhir/StructureDefinition/AIC-IntraOperativeObservation
      conceptMapContext.concepts("9187-6").head("source_system") shouldBe "http://loinc.org"
      conceptMapContext.concepts("9187-6").head("unit") shouldBe "cm3"

      conceptMapContext.concepts("1234-5").length shouldBe 2
      conceptMapContext.concepts("1234-5").head("source_system") shouldBe "http://loinc.org"
      conceptMapContext.concepts("1234-5").head("source_display") shouldBe "Hemogoblin"
      conceptMapContext.concepts("1234-5").head("unit") shouldBe "g/dL"

      conceptMapContext.concepts("1234-5")(1)("source_system") shouldBe "http://loinc.org"
      conceptMapContext.concepts("1234-5")(1)("source_display") shouldBe "Hemogoblin X"
      conceptMapContext.concepts("1234-5")(1)("unit") shouldBe "g/cL"
    }
  }

  it should "correctly load concept map context definitions from another directory" in {
    val labResultsMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/lab-results-mapping")
    val contextDefinition = labResultsMapping.context("obsConceptMap")
    val mappingContextLoader = new MappingContextLoader
    mappingContextLoader.retrieveContext(contextDefinition) map { context =>
      val conceptMapContext = context.asInstanceOf[ConceptMapContext]
      conceptMapContext.concepts.size shouldBe 14

      // source_code,source_system,source_display,unit,profile
      // 9187-6,http://loinc.org,Urine Output,cm3,https://aiccelerate.eu/fhir/StructureDefinition/AIC-IntraOperativeObservation
      conceptMapContext.concepts("9187-6").head("source_system") shouldBe "http://loinc.org"
      conceptMapContext.concepts("9187-6").head("unit") shouldBe "cm3"
    }
  }

  it should "correctly load unit conversion mapping definitions" in {
    val labResultsMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/lab-results-mapping")
    val unitConversionContextDefinition = labResultsMapping.context("labResultUnitConversion")
    val mappingContextLoader = new MappingContextLoader
    mappingContextLoader.retrieveContext(unitConversionContextDefinition) map { context =>
      val unitConversionContext = context.asInstanceOf[UnitConversionContext]
      unitConversionContext.conversionFunctions.size shouldBe 25

      // source_code,source_unit,target_unit,conversion_function
      // 10207,mmol/l,mmol/L,"""$this"""
      val conversionValue = unitConversionContext.conversionFunctions("10207", "mmol/l")
      conversionValue._1 shouldBe "mmol/L"
      conversionValue._2 shouldBe "$this"
    }
  }

}

