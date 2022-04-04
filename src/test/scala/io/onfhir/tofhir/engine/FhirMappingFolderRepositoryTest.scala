package io.onfhir.tofhir.engine

import io.onfhir.tofhir.ToFhirTestSpec
import org.json4s.{JObject, JValue}

class FhirMappingFolderRepositoryTest extends ToFhirTestSpec {

  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(getClass.getResource("/test-mappings-1").getFile)

  "A FhirMappingRepository" should "correctly read and parse the mapping files under the given mappings folder" in {
    val patientMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/patient-mapping")
    patientMapping.name shouldBe "patient-mapping"
    patientMapping.mapping.length shouldBe 1
    patientMapping.mapping.head.expression.value.isEmpty shouldBe false

    val observationMapping = mappingRepository.getFhirMappingByUrl("https://aiccelerate.eu/fhir/mappings/other-observation-mapping")
    observationMapping.name shouldBe "other-observation-mapping"
    observationMapping.mapping.length shouldBe 3
    observationMapping.mapping.head.expression.value.isEmpty shouldBe false
  }

}
