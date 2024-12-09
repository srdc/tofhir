import akka.actor.ActorSystem
import io.onfhir.definitions.common.model.Json4sSupport.formats
import io.tofhir.rxnorm.RxNormApiClient
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RxNormApiClientTest extends AnyFlatSpec with Matchers {
  implicit val actorSystem = ActorSystem("test")
  val client = new RxNormApiClient("https://rxnav.nlm.nih.gov", 10)

  "RxNormApiClient" should "get corresponding RxNorm CUI for given NDC" in {
    client.findRxConceptIdByNdc("63739054410") shouldBe Seq("313096")
  }

  it should "not found a non existent NDC" in {
    client.findRxConceptIdByNdc("123") shouldBe Nil
  }

  it should "get the details of the medication with with concept id" in {
    client.getRxcuiHistoryStatus("603748").isDefined shouldBe true
  }

  it should "get the ingredients for given RxNorm concept id" in {
    val result = client.getIngredientProperties("476556")
    result.length shouldBe 2
    (result.head \ "Active_ingredient_RxCUI").extract[String] shouldBe "276237"
    (result.head \ "Active_ingredient_name").extract[String] shouldBe "emtricitabine"
    (result.head \ "Numerator_Value").extract[Int] shouldBe 200
    (result.head \ "Numerator_Units").extract[String] shouldBe "MG"
  }

  it should "not found for a non existent id" in {
    val result = client.getIngredientProperties("476")
    result.length shouldBe 0
  }

  it should "get the ATC code for given RxNorm concept id" in {
    client.getAtcCode("276237") shouldBe Seq("J05AF09")
  }

}
