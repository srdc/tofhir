package io.onfhir.tofhir.engine

import io.onfhir.api.util.FHIRUtil
import io.onfhir.tofhir.ToFhirTestSpec
import io.onfhir.tofhir.config.{MappingErrorHandling, ToFhirConfig}
import io.onfhir.tofhir.model.{FhirMappingTask, SqlSource, SqlSourceSettings}
import io.onfhir.tofhir.util.FhirMappingUtility

class SqlSourceTest extends ToFhirTestSpec {

  val sqlSourceSettings: SqlSourceSettings = SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data",
    databaseUrl = "jdbc:postgresql://localhost:5432/omop", username = "srdc", password = "srdc")

  val patientMappingTask: FhirMappingTask = FhirMappingTask(
    mappingRef = "https://aiccelerate.eu/fhir/mappings/patient-mapping",
    sourceContext = Map("source" -> SqlSource("patients", sqlSourceSettings)))

  "Patient mapping" should "should read data from SQL source and map it" in {
    val fhirMappingJobManager = new FhirMappingJobManager(mappingRepository, contextLoader, schemaRepository, sparkSession, MappingErrorHandling.withName(ToFhirConfig.mappingErrorHandling))
    fhirMappingJobManager.executeMappingTaskAndReturn(task = patientMappingTask) map { results =>
      results.size shouldBe 2
      val patient1 = results.head
      FHIRUtil.extractResourceType(patient1) shouldBe "Patient"
      FHIRUtil.extractIdFromResource(patient1) shouldBe FhirMappingUtility.getHashedId("Patient", "p1")
      FHIRUtil.extractValue[String](patient1, "gender") shouldBe "male"
    }
  }
}
