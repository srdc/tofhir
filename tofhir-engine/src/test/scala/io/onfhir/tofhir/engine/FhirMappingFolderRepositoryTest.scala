package io.onfhir.tofhir.engine

import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.model.{ConceptMapContext, FhirMappingException, UnitConversionContext}

import java.io.File
import java.net.URI

class FhirMappingFolderRepositoryTest extends ToFhirTestSpec {

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
    val mappingContextLoader = new MappingContextLoader(mappingRepository)
    mappingContextLoader.retrieveContext(contextDefinition) map { context =>
      val conceptMapContext = context.asInstanceOf[ConceptMapContext]
      conceptMapContext.concepts.size shouldBe 13

      // source_code,source_system,source_display,unit,profile
      // 9187-6,http://loinc.org,Urine Output,cm3,https://aiccelerate.eu/fhir/StructureDefinition/AIC-IntraOperativeObservation
      conceptMapContext.concepts("9187-6")("source_system") shouldBe "http://loinc.org"
      conceptMapContext.concepts("9187-6")("unit") shouldBe "cm3"
    }
  }

  it should "correctly load concept map context definitions from another directory" in {
    val labResultsMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/lab-results-mapping")
    val contextDefinition = labResultsMapping.context("obsConceptMap")
    val mappingContextLoader = new MappingContextLoader(mappingRepository)
    mappingContextLoader.retrieveContext(contextDefinition) map { context =>
      val conceptMapContext = context.asInstanceOf[ConceptMapContext]
      conceptMapContext.concepts.size shouldBe 13

      // source_code,source_system,source_display,unit,profile
      // 9187-6,http://loinc.org,Urine Output,cm3,https://aiccelerate.eu/fhir/StructureDefinition/AIC-IntraOperativeObservation
      conceptMapContext.concepts("9187-6")("source_system") shouldBe "http://loinc.org"
      conceptMapContext.concepts("9187-6")("unit") shouldBe "cm3"
    }
  }

  it should "correctly load unit conversion mapping definitions" in {
    val labResultsMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/lab-results-mapping")
    val unitConversionContextDefinition = labResultsMapping.context("labResultUnitConversion")
    val mappingContextLoader = new MappingContextLoader(mappingRepository)
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

