package io.tofhir.test

import io.onfhir.api.util.FHIRUtil
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.ToFhirTestSpec
import io.tofhir.engine.mapping.service.LocalTerminologyService
import io.tofhir.engine.model.{CodeSystemFile, ConceptMapFile, LocalFhirTerminologyServiceSettings}
import org.scalatest.flatspec.AsyncFlatSpec

import java.nio.file.Paths
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.Await

class LocalTerminologyServiceTest extends AsyncFlatSpec with ToFhirTestSpec {

  val terminologyServiceFolderPath: String = Paths.get(getClass.getResource("/terminology-service").toURI).normalize().toAbsolutePath.toString
  val settings: LocalFhirTerminologyServiceSettings = LocalFhirTerminologyServiceSettings(terminologyServiceFolderPath,
    conceptMapFiles = Seq(
      ConceptMapFile("sample-concept-map.csv", "sample-concept-map.csv", "http://example.com/fhir/ConceptMap/sample1", "http://terminology.hl7.org/ValueSet/v2-0487", "http://snomed.info/sct?fhir_vs")
    ),
    codeSystemFiles = Seq(
      CodeSystemFile("sample-concept-map.csv", "sample-code-system.csv", "http://snomed.info/sct")
    )
  )
  val localTerminologyService = new LocalTerminologyService(settings)

  "A LocalTerminologyService" should "translate code via concept url with single match" in {
    var result = Await.result(localTerminologyService.translate("ACNE", "http://terminology.hl7.org/CodeSystem/v2-0487", "http://example.com/fhir/ConceptMap/sample1"), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(true)
    FHIRUtil
      .getParameterValueByPath(result, "match.concept")
      .map(c => (c \ "code").extract[String]) shouldEqual Seq("309068002")

    result = Await.result(localTerminologyService.translate("ACNE", "http://terminology.hl7.org/CodeSystem/v2-0487", "http://example.com/fhir/ConceptMap/sample1", None, None, false), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(true)
  }

  it should "translate code via concept url with multiple match" in {
    val result = Await.result(localTerminologyService.translate("ACNFLD", "http://terminology.hl7.org/CodeSystem/v2-0487", "http://example.com/fhir/ConceptMap/sample1"), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(true)
    FHIRUtil
      .getParameterValueByPath(result, "match.concept")
      .map(c => (c \ "code").extract[String]).toSet shouldEqual Set("119323008", "119323009")
  }

  it should "return no translation via concept url if no code is matching" in {
    val result = Await.result(localTerminologyService.translate("XYZ", "http://terminology.hl7.org/CodeSystem/v2-0487", "http://example.com/fhir/ConceptMap/sample1"), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(false)
  }

  it should "return no translation via concept url if no system is matching" in {
    val result = Await.result(localTerminologyService.translate("ACNFLD", "NO-MATCHING", "http://example.com/fhir/ConceptMap/sample1"), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(false)
  }

  it should "return no translation via concept url if no conceptUrl" in {
    val result = Await.result(localTerminologyService.translate("ACNFLD", "http://terminology.hl7.org/CodeSystem/v2-0487", "http://example.com/fhir/ConceptMap/NO"), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(false)
  }

  it should "translate code via source and target value set" in {
    var result = Await.result(localTerminologyService.translate("ACNE", "http://terminology.hl7.org/CodeSystem/v2-0487", Some("http://terminology.hl7.org/ValueSet/v2-0487"), Some("http://snomed.info/sct?fhir_vs")), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(true)
    FHIRUtil
      .getParameterValueByPath(result, "match.concept")
      .map(c => (c \ "code").extract[String]) shouldEqual Seq("309068002")

    result = Await.result(localTerminologyService.translate("ACNE", "http://terminology.hl7.org/CodeSystem/v2-0487", Some("http://terminology.hl7.org/ValueSet/v2-0487"), Some("http://snomed.info/sct?fhir_vs"), None, false), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(true)
  }

  it should "translate CodeableConcept via  concept url" in {
    val codeableConcept =
      """{
        |		"coding": [
        |			{
        |				"system": "http://terminology.hl7.org/CodeSystem/v2-0487",
        |				"code": "ACNE",
        |				"display": "Negative"
        |			}, {
        |				"system": "http://terminology.hl7.org/CodeSystem/v2-0487",
        |				"code": "NEG",
        |				"display": "Negative"
        |			}
        |		],
        |		"text": "Negative for Chlamydia Trachomatis rRNA"
        |	}""".stripMargin
    import io.onfhir.util.JsonFormatter._
    val result = Await.result(localTerminologyService.translate(codeableConcept.parseJson, "http://example.com/fhir/ConceptMap/sample1"), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(true)
    FHIRUtil
      .getParameterValueByPath(result, "match.concept")
      .map(c => (c \ "code").extract[String]) shouldEqual Seq("309068002")
  }

  it should "translate CodeableConcept via  concept url with multiple matchings" in {
    val codeableConcept =
      """{
        |		"coding": [
        |			{
        |				"system": "http://terminology.hl7.org/CodeSystem/v2-0487",
        |				"code": "ACNE",
        |				"display": "Negative"
        |			}, {
        |				"system": "http://terminology.hl7.org/CodeSystem/v2-0487",
        |				"code": "ACNFLD",
        |				"display": "Negative"
        |			}
        |		],
        |		"text": "Negative for Chlamydia Trachomatis rRNA"
        |	}""".stripMargin
    import io.onfhir.util.JsonFormatter._
    val result = Await.result(localTerminologyService.translate(codeableConcept.parseJson, "http://example.com/fhir/ConceptMap/sample1"), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(true)
    FHIRUtil
      .getParameterValueByPath(result, "match.concept")
      .map(c => (c \ "code").extract[String]) shouldEqual Seq("309068002", "119323008", "119323009")
  }

  it should "translate Coding via  concept url" in {
    val coding =
      """	{
        |				"system": "http://terminology.hl7.org/CodeSystem/v2-0487",
        |				"code": "ACNE",
        |				"display": "Negative"
        |			}""".stripMargin
    import io.onfhir.util.JsonFormatter._
    val result = Await.result(localTerminologyService.translate(coding.parseJson, "http://example.com/fhir/ConceptMap/sample1"), FiniteDuration(5, TimeUnit.SECONDS))
    FHIRUtil.getParameterValueByName(result, "result").map(_.extract[Boolean]) shouldEqual Some(true)
    FHIRUtil
      .getParameterValueByPath(result, "match.concept")
      .map(c => (c \ "code").extract[String]) shouldEqual Seq("309068002")
  }

  it should "lookup a code that exist" in {
    val result = Await.result(localTerminologyService.lookup("119323008", "http://snomed.info/sct"), FiniteDuration(5, TimeUnit.SECONDS))
    result.isEmpty shouldBe (false)
    FHIRUtil.getParameterValueByName(result.get, "display").map(_.extract[String]) shouldBe Some("Pus specimen")
  }

  it should "lookup a code with a language that exist" in {
    val result = Await.result(localTerminologyService.lookup("119323008", "http://snomed.info/sct", None, None, Some("de"), Nil), FiniteDuration(5, TimeUnit.SECONDS))
    result.isEmpty shouldBe (false)
    FHIRUtil.getParameterValueByName(result.get, "display").map(_.extract[String]) shouldBe Some("Eiterprobe")
  }

  it should "return empty for a code that does not exist" in {
    val result = Await.result(localTerminologyService.lookup("123", "http://snomed.info/sct"), FiniteDuration(5, TimeUnit.SECONDS))
    result.isEmpty shouldBe (true)
  }

  it should "lookup a code with a language where designation does not exist (returning default display)" in {
    val result = Await.result(localTerminologyService.lookup("119323008", "http://snomed.info/sct", None, None, Some("tr"), Nil), FiniteDuration(5, TimeUnit.SECONDS))
    result.isEmpty shouldBe (false)
    FHIRUtil.getParameterValueByName(result.get, "display").map(_.extract[String]) shouldBe Some("Pus specimen")
  }

  it should "return empty if system does not exist" in {
    val result = Await.result(localTerminologyService.lookup("123", "loinc"), FiniteDuration(5, TimeUnit.SECONDS))
    result.isEmpty shouldBe (true)
  }
}
