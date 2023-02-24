package io.tofhir.server.terminology

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.model.{TerminologyCodeSystem, TerminologyConceptMap, TerminologySystem}
import io.tofhir.server.service.terminology.TerminologySystemFolderRepository
import io.tofhir.server.util.FileOperations
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

import java.io.File

class TerminologyServiceManagerEndpointTest extends BaseEndpointTest {
  var conceptMap1: TerminologyConceptMap = TerminologyConceptMap(name = "testCM", conceptMapUrl = "", sourceValueSetUrl = "", targetValueSetUrl = "")
  var conceptMap2: TerminologyConceptMap = TerminologyConceptMap(name = "testCM2", conceptMapUrl = "", sourceValueSetUrl = "", targetValueSetUrl = "")

  var codeSystem1: TerminologyCodeSystem = TerminologyCodeSystem(name = "testCS", codeSystem = "")
  var codeSystem2: TerminologyCodeSystem = TerminologyCodeSystem(name = "testCS2", codeSystem = "")

  var terminologySystem1: TerminologySystem = TerminologySystem(name = "testTerminology1", description = "example terminology 1", codeSystems = Seq.empty, conceptMaps = Seq.empty)

  val terminologySystem2: TerminologySystem = TerminologySystem(name = "testTerminology2", description = "example terminology 2", codeSystems = Seq.empty, conceptMaps = Seq.empty)

  "The terminology service" should {
    "create a terminology system" in {
      // create the first terminology
      Post(s"/${webServerConfig.baseUri}/terminologies", HttpEntity(ContentTypes.`application/json`, writePretty(terminologySystem1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        val terminologySystem: TerminologySystem = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        // set the created terminology
        terminologySystem1 = terminologySystem
        // validate that a folder is created for the terminology
        new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_FOLDER + File.separatorChar + terminologySystem.id).exists() shouldEqual true
        // validate that terminologies metadata file is updated
        val terminologySystems
        = FileOperations.readJsonContent[TerminologySystem](new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_JSON))
        terminologySystems
          .length shouldEqual 1
      }
      // create the second terminology
      Post(s"/${webServerConfig.baseUri}/terminologies", HttpEntity(ContentTypes.`application/json`, writePretty(terminologySystem2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that terminologies metadata file is updated
        val terminologySystems
        = FileOperations.readJsonContent[TerminologySystem](new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_JSON))
        terminologySystems
          .length shouldEqual 2
      }
    }

    "get all terminology systems" in {
      // retrieve all terminologies
      Get(s"/${webServerConfig.baseUri}/terminologies") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two terminologies
        val terminologySystems
        : Seq[TerminologySystem] = JsonMethods.parse(responseAs[String]).extract[Seq[TerminologySystem]]
        terminologySystems
          .length shouldEqual 2
      }
    }

    "get a terminology system" in {
      // get a terminology system
      Get(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved terminology system
        val terminologySystem: TerminologySystem = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        terminologySystem.id shouldEqual terminologySystem1.id
        terminologySystem.name shouldEqual terminologySystem1.name
      }
    }

    "put a terminology system" in {
      // create a copy of the first terminology and update its name and check if it is updated
      Put(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(terminologySystem1.copy(name = "nameUpdated1")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned terminology system includes the update
        val terminologySystem: TerminologySystem = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        terminologySystem.name shouldEqual "nameUpdated1"
        // validate that terminology systems is updated
        val terminologySystems
        = FileOperations.readJsonContent[TerminologySystem](new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_JSON))
        terminologySystems
          .find(_.id == terminologySystem.id).get.name shouldEqual "nameUpdated1"
        terminologySystem1 = terminologySystem
      }
    }

    "delete a terminology system" in {
      // delete a terminology system
      Delete(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem2.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that terminology folder is deleted
        new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_FOLDER + File.separatorChar + terminologySystem2.id).exists() shouldEqual false
        // validate that terminology metadata file is updated
        val terminologySystems
        = FileOperations.readJsonContent[TerminologySystem](new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_JSON))
        terminologySystems
          .length shouldEqual 1
      }
      // delete a non-existent terminology system
      Delete(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem2.id}") ~> route ~> check {
        status should not equal StatusCodes.OK
      }
    }
  }

  "The terminology concept map service" should {
    "create a concept map within a terminology system" in {
      // create a concept map
      Post(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/concept-maps", HttpEntity(ContentTypes.`application/json`, writePretty(conceptMap1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that the returned concept map includes the id
        val conceptMap: TerminologyConceptMap = JsonMethods.parse(responseAs[String]).extract[TerminologyConceptMap]
        conceptMap.name shouldEqual "testCM"
        // validate that concept map file is created
        new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_FOLDER + File.separatorChar +
          terminologySystem1.id + File.separatorChar + conceptMap.id).exists() shouldEqual true
      }
      // create a second concept map within same terminology
      Post(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/concept-maps", HttpEntity(ContentTypes.`application/json`, writePretty(conceptMap2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that the returned concept map includes the id
        val conceptMap: TerminologyConceptMap = JsonMethods.parse(responseAs[String]).extract[TerminologyConceptMap]
        conceptMap.name shouldEqual "testCM2"
        // validate that concept map file is created
        new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_FOLDER + File.separatorChar +
          terminologySystem1.id + File.separatorChar + conceptMap.id).exists() shouldEqual true
      }
    }

    "get concept maps within a terminology system" in {
      // get a concept map list within a terminology system
      Get(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/concept-maps") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns one concept maps
        val conceptMaps: Seq[TerminologyConceptMap] = JsonMethods.parse(responseAs[String]).extract[Seq[TerminologyConceptMap]]
        conceptMaps.length shouldEqual 2
      }
    }

    "get a concept map within a terminology system" in {
      // get a concept map within a terminology system
      Get(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/concept-maps/${conceptMap1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns the concept map
        val conceptMap: TerminologyConceptMap = JsonMethods.parse(responseAs[String]).extract[TerminologyConceptMap]
        conceptMap.name shouldEqual conceptMap1.name
      }
    }

    "put a concept map within a terminology system" in {
      // update a concept map within a terminology system
      Put(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/concept-maps/${conceptMap1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(conceptMap1.copy(name = "testCMUpdated")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned concept map includes the update
        val conceptMap: TerminologyConceptMap = JsonMethods.parse(responseAs[String]).extract[TerminologyConceptMap]
        conceptMap.name shouldEqual "testCMUpdated"
        conceptMap1 = conceptMap
      }
    }

    "delete a concept map within a terminology system" in {
      // delete a concept map within a terminology system
      Delete(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/concept-maps/${conceptMap2.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that concept map file is deleted
        new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_FOLDER + File.separatorChar +
          terminologySystem1.name + File.separatorChar + conceptMap2.id).exists() shouldEqual false
      }
      // delete a non-existent concept map within a terminology system
      Delete(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/concept-maps/${conceptMap2.id}") ~> route ~> check {
        status should not equal StatusCodes.OK
      }
    }

    "upload a csv content as a concept map within a terminology system" in {
      // get file from resources
      val file: File = FileOperations.getFileIfExists(getClass.getResource("/sample-concept-map.csv").getPath)
      val fileData = Multipart.FormData.BodyPart.fromPath("attachment", ContentTypes.`text/plain(UTF-8)`, file.toPath)
      val formData = Multipart.FormData(fileData)
      // save a csv file as a concept map within a terminology system
      Post(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/concept-maps/${conceptMap1.id}/content", formData.toEntity()) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "OK"
        Thread.sleep(5000)
      }
    }


    "download csv content of a concept map within a terminology system" in {
      // download csv content of a concept map within a terminology system
      Get(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/concept-maps/${conceptMap1.id}/content") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns the csv content
        val csvContent: String = responseAs[String]
        // remove new lines to compare without system specific line endings
        csvContent.replaceAll("\\r", "").replaceAll("\\n", "") shouldEqual "source_system,source_code,target_system,target_code,target_display,equivalence" +
          "http://terminology.hl7.org/CodeSystem/v2-0487,ACNE,http://snomed.info/sct,309068002,Specimen from skin,equivalent" +
          "http://terminology.hl7.org/CodeSystem/v2-0487,ACNFLD,http://snomed.info/sct,119323008,Pus specimen,equivalent" +
          "http://terminology.hl7.org/CodeSystem/v2-0487,ACNFLD,http://snomed.info/sct,119323009,Pus specimen 2,equivalent" +
          "http://terminology.hl7.org/CodeSystem/v2-0487,BULLA,http://snomed.info/sct,258482009,Pus specimen 2,narrower"
      }
    }
  }

  "The terminology code system service" should {
    "create a code system within a terminology system" in {
      // create a code system
      Post(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/code-systems", HttpEntity(ContentTypes.`application/json`, writePretty(codeSystem1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that the returned code system includes the id
        val codeSystem: TerminologyCodeSystem = JsonMethods.parse(responseAs[String]).extract[TerminologyCodeSystem]
        codeSystem.name shouldEqual "testCS"
        // validate that code system file is created
        new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_FOLDER + File.separatorChar +
          terminologySystem1.id + File.separatorChar + codeSystem.id).exists() shouldEqual true
      }
      // create a second code system within same terminology
      Post(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/code-systems", HttpEntity(ContentTypes.`application/json`, writePretty(codeSystem2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that the returned code system includes the id
        val codeSystem: TerminologyCodeSystem = JsonMethods.parse(responseAs[String]).extract[TerminologyCodeSystem]
        codeSystem.name shouldEqual "testCS2"
        // validate that code system file is created
        new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_FOLDER + File.separatorChar +
          terminologySystem1.id + File.separatorChar + codeSystem.id).exists() shouldEqual true
      }
    }

    "get code systems within a terminology system" in {
      // get a code system list within a terminology system
      Get(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/code-systems") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns one code systems
        val codeSystems: Seq[TerminologyCodeSystem] = JsonMethods.parse(responseAs[String]).extract[Seq[TerminologyCodeSystem]]
        codeSystems.length shouldEqual 2
      }
    }

    "get a code system within a terminology system" in {
      // get a code system within a terminology system
      Get(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/code-systems/${codeSystem1.id}").addHeader(RawHeader("Content-Type", "application/json")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns the code system
        val codeSystem: TerminologyCodeSystem = JsonMethods.parse(responseAs[String]).extract[TerminologyCodeSystem]
        codeSystem.name shouldEqual codeSystem1.name
      }
    }

    "put a code system within a terminology system" in {
      // update a code system within a terminology system
      Put(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/code-systems/${codeSystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(codeSystem1.copy(name = "testCSUpdated")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned code system includes the update
        val codeSystem: TerminologyCodeSystem = JsonMethods.parse(responseAs[String]).extract[TerminologyCodeSystem]
        codeSystem.name shouldEqual "testCSUpdated"
      }
    }

    "delete a code system within a terminology system" in {
      // delete a code system within a terminology system
      Delete(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/code-systems/${codeSystem2.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that code system file is deleted
        new File(TerminologySystemFolderRepository.TERMINOLOGY_SYSTEMS_FOLDER + File.separatorChar +
          terminologySystem1.id + File.separatorChar + codeSystem2.id).exists() shouldEqual false
      }
      // delete a non-existent code system within a terminology system
      Delete(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/code-systems/${codeSystem2.id}") ~> route ~> check {
        status should not equal StatusCodes.OK
      }
    }

    "upload a csv content as a code system within a terminology system" in {
      // get file from resources
      val file: File = FileOperations.getFileIfExists(getClass.getResource("/sample-code-system.csv").getPath)
      val fileData = Multipart.FormData.BodyPart.fromPath("attachment", ContentTypes.`text/plain(UTF-8)`, file.toPath)
      val formData = Multipart.FormData(fileData)
      // save a csv file as a concept map within a terminology system
      Post(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/code-systems/${codeSystem1.id}/content", formData.toEntity()) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "OK"
        Thread.sleep(5000)
      }
    }

    "download csv content of a code system within a terminology system" in {
      // download csv content of a code system within a terminology system
      Get(s"/${webServerConfig.baseUri}/terminologies/${terminologySystem1.id}/code-systems/${codeSystem1.id}/content") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns the csv content
        val csvContent: String = responseAs[String]
        // remove new lines to compare without system specific line endings
        csvContent.replaceAll("\\r", "").replaceAll("\\n", "") shouldEqual "code,display,fr,de" +
          "309068002,Specimen from skin,Spécimen de peau,Probe von der Haut" +
          "119323008,Pus specimen,Spécimen de pus,Eiterprobe"
      }
    }
  }
}
