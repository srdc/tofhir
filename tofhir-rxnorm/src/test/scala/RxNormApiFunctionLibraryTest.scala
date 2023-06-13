import io.onfhir.path.FhirPathEvaluator
import io.tofhir.rxnorm.RxNormApiFunctionLibraryFactory
import org.json4s.JsonAST.JNull
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RxNormApiFunctionLibraryTest extends AnyFlatSpec with Matchers {
  val rxNormApiFunctionLibraryFactory = new RxNormApiFunctionLibraryFactory("https://rxnav.nlm.nih.gov", 10)
  val fhirPathEvaluator = FhirPathEvaluator.apply().withDefaultFunctionLibraries().withFunctionLibrary("rxn", rxNormApiFunctionLibraryFactory)

  "RxNormApiFunctionLibrary" should "handle findRxConceptIdsByNdc" in {
    fhirPathEvaluator.evaluateOptionalString("rxn:findRxConceptIdsByNdc(63739054410)", JNull) shouldBe Some("313096")
  }
}
