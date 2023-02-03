package io.tofhir.server
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.server.endpoint.LocalTerminologyEndpoint
import io.tofhir.server.model.{LocalTerminology, TerminologyCodeSystem, TerminologyConceptMap, ToFhirRestCall}
import io.tofhir.server.service.localterminology.LocalTerminologyFolderRepository
import io.tofhir.server.util.FileOperations
import org.apache.commons.io.FileUtils
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.{File, FileWriter}

class LocalTerminologyEndpointTest extends AnyWordSpec with Matchers with ScalatestRouteTest with BeforeAndAfterAll {

  // toFHIR engine config
  val toFhirEngineConfig: ToFhirEngineConfig = new ToFhirEngineConfig(system.settings.config.getConfig("tofhir"))
  // local terminology endpoint to be tested
  val localTerminologyEndpoint: LocalTerminologyEndpoint = new LocalTerminologyEndpoint(toFhirEngineConfig)
  // route of local terminology endpoint, it is initialized with dummy rest call
  val route: Route = localTerminologyEndpoint.route(new ToFhirRestCall(HttpMethod.custom("GET"), "", "", HttpEntity(ContentTypes.`application/json`, "")))

  var conceptMap1: TerminologyConceptMap = TerminologyConceptMap(name = "testCM", conceptMapUrl = "", sourceValueSetUrl = "", targetValueSetUrl = "")
  var conceptMap2: TerminologyConceptMap = TerminologyConceptMap(name = "testCM2", conceptMapUrl = "", sourceValueSetUrl = "", targetValueSetUrl = "")

  var codeSystem1: TerminologyCodeSystem = TerminologyCodeSystem(name = "testCS", codeSystem = "")
  var codeSystem2: TerminologyCodeSystem = TerminologyCodeSystem(name = "testCS2", codeSystem = "")

  var localTerminology1: LocalTerminology = LocalTerminology(name = "testTerminology1", description = "example terminology 1", codeSystems = Seq.empty, conceptMaps = Seq.empty)

  val localTerminology2: LocalTerminology = LocalTerminology(name = "testTerminology2", description = "example terminology 2", codeSystems = Seq.empty, conceptMaps = Seq.empty)

  /**
   * Creates a repository folder and terminology-services folder before tests are run.
   * */
  override def beforeAll(): Unit = {
    new File(toFhirEngineConfig.contextPath).mkdir()
    new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER).mkdir()
    val terminologyServicesJson = new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_JSON)
    terminologyServicesJson.createNewFile()
    val writer = new FileWriter(terminologyServicesJson)
    try writer.write(writePretty(Seq.empty)) finally writer.close() // write empty array to the file
  }

  /**
   * Deletes the repository folder after all test cases are completed.
   * */
  override def afterAll(): Unit = {
    FileUtils.deleteDirectory(new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER))
    val terminologyServicesJson = new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_JSON)
    terminologyServicesJson.delete()
  }

  "The terminology service" should {
    "create a local terminology" in {
      // create the first terminology
      Post("/terminology", HttpEntity(ContentTypes.`application/json`, writePretty(localTerminology1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        val localTerminology: LocalTerminology = JsonMethods.parse(responseAs[String]).extract[LocalTerminology]
        // set the created terminology
        localTerminology1 = localTerminology
        // validate that a folder is created for the terminology
        new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar + localTerminology.name).exists() shouldEqual true
        // validate that terminologies metadata file is updated
        val localTerminologies = FileOperations.readJsonContent(new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_JSON), classOf[LocalTerminology])
        localTerminologies.length shouldEqual 1
      }
      // create the second terminology
      Post("/terminology", HttpEntity(ContentTypes.`application/json`, writePretty(localTerminology2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that terminologies metadata file is updated
        val localTerminologies = FileOperations.readJsonContent(new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_JSON), classOf[LocalTerminology])
        localTerminologies.length shouldEqual 2
      }
    }

    "get all local terminologies" in {
      // retrieve all terminologies
      Get("/terminology") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two terminologies
        val localTerminologies: Seq[LocalTerminology] = JsonMethods.parse(responseAs[String]).extract[Seq[LocalTerminology]]
        localTerminologies.length shouldEqual 2
      }
    }

    "get a local terminology" in {
      // get a local terminology
      Get(s"/terminology/${localTerminology1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved local terminology
        val localTerminology: LocalTerminology = JsonMethods.parse(responseAs[String]).extract[LocalTerminology]
        localTerminology.id shouldEqual localTerminology1.id
        localTerminology.name shouldEqual localTerminology1.name
      }
    }

    "put a local terminology" in {
      // create a copy of the first terminology and update its name and check if it is updated
      Put(s"/terminology/${localTerminology1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(localTerminology1.copy(name = "nameUpdated1")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned local terminology includes the update
        val localTerminology: LocalTerminology = JsonMethods.parse(responseAs[String]).extract[LocalTerminology]
        localTerminology.name shouldEqual "nameUpdated1"
        // validate that local terminologies is updated
        val localTerminologies = FileOperations.readJsonContent(new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_JSON), classOf[LocalTerminology])
        localTerminologies.find(_.id == localTerminology.id).get.name shouldEqual "nameUpdated1"
        // validate that folder name is updated for the terminology
        new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar + localTerminology.name).exists() shouldEqual true
        localTerminology1 = localTerminology
      }
    }

    "delete a local terminology" in {
      // delete a local terminology
      Delete(s"/terminology/${localTerminology2.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that terminology folder is deleted
        new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar + localTerminology2.name).exists() shouldEqual false
        // validate that terminology metadata file is updated
        val localTerminologies = FileOperations.readJsonContent(new File(toFhirEngineConfig.contextPath + File.separatorChar + LocalTerminologyFolderRepository.TERMINOLOGY_JSON), classOf[LocalTerminology])
        localTerminologies.length shouldEqual 1
      }
      // delete a non-existent local terminology
      Delete(s"/terminology/${localTerminology2.id}") ~> route ~> check {
        status should not equal StatusCodes.OK
      }
    }
  }

  "The terminology concept map service" should {
    "create a concept map within a local terminology" in {
      // create a concept map
      Post(s"/terminology/${localTerminology1.id}/conceptmap", HttpEntity(ContentTypes.`application/json`, writePretty(conceptMap1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that the returned concept map includes the id
        val conceptMap: TerminologyConceptMap = JsonMethods.parse(responseAs[String]).extract[TerminologyConceptMap]
        conceptMap.name shouldEqual "testCM"
        // validate that concept map file is created
        new File(toFhirEngineConfig.contextPath + File.separatorChar +
          LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar +
          localTerminology1.name + File.separatorChar + conceptMap.name).exists() shouldEqual true
      }
      // create a second concept map within same terminology
      Post(s"/terminology/${localTerminology1.id}/conceptmap", HttpEntity(ContentTypes.`application/json`, writePretty(conceptMap2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that the returned concept map includes the id
        val conceptMap: TerminologyConceptMap = JsonMethods.parse(responseAs[String]).extract[TerminologyConceptMap]
        conceptMap.name shouldEqual "testCM2"
        // validate that concept map file is created
        new File(toFhirEngineConfig.contextPath + File.separatorChar +
          LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar +
          localTerminology1.name + File.separatorChar + conceptMap.name).exists() shouldEqual true
      }
    }

    "get concept maps within a local terminology" in {
      // get a concept map list within a local terminology
      Get(s"/terminology/${localTerminology1.id}/conceptmap") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns one concept maps
        val conceptMaps: Seq[TerminologyConceptMap] = JsonMethods.parse(responseAs[String]).extract[Seq[TerminologyConceptMap]]
        conceptMaps.length shouldEqual 2
      }
    }

    "get a concept map within a local terminology" in {
      // get a concept map within a local terminology
      Get(s"/terminology/${localTerminology1.id}/conceptmap/${conceptMap1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns the concept map
        val conceptMap: TerminologyConceptMap = JsonMethods.parse(responseAs[String]).extract[TerminologyConceptMap]
        conceptMap.name shouldEqual conceptMap1.name
      }
    }

    "put a concept map within a local terminology" in {
      // update a concept map within a local terminology
      Put(s"/terminology/${localTerminology1.id}/conceptmap/${conceptMap1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(conceptMap1.copy(name = "testCMUpdated")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned concept map includes the update
        val conceptMap: TerminologyConceptMap = JsonMethods.parse(responseAs[String]).extract[TerminologyConceptMap]
        conceptMap.name shouldEqual "testCMUpdated"
        // validate that concept map file is updated
        new File(toFhirEngineConfig.contextPath + File.separatorChar +
          LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar +
          localTerminology1.name + File.separatorChar + conceptMap.name).exists() shouldEqual true

        // validate that old concept map file is deleted
        new File(toFhirEngineConfig.contextPath + File.separatorChar +
          LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar +
          localTerminology1.name + File.separatorChar + conceptMap1.name).exists() shouldEqual false
        conceptMap1 = conceptMap
      }
    }

    "delete a concept map within a local terminology" in {
      // delete a concept map within a local terminology
      Delete(s"/terminology/${localTerminology1.id}/conceptmap/${conceptMap2.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that concept map file is deleted
        new File(toFhirEngineConfig.contextPath + File.separatorChar +
          LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar +
          localTerminology1.name + File.separatorChar + conceptMap2.name).exists() shouldEqual false
      }
      // delete a non-existent concept map within a local terminology
      Delete(s"/terminology/${localTerminology1.id}/conceptmap/${conceptMap2.id}") ~> route ~> check {
        status should not equal StatusCodes.OK
      }
    }

    "upload a csv content as a concept map within a local terminology" in {
      // get file from resources
      val file: File = FileOperations.getFileIfExists(getClass.getResource("/sample-concept-map.csv").getPath)
      val fileData = Multipart.FormData.BodyPart.fromPath("attachment", ContentTypes.`text/plain(UTF-8)`, file.toPath)
      val formData = Multipart.FormData(fileData)
      // save a csv file as a concept map within a local terminology
      Post(s"/terminology/${localTerminology1.id}/conceptmap/${conceptMap1.id}/content", formData.toEntity()) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "OK"
      }
    }


    "download csv content of a concept map within a local terminology" in {
      // download csv content of a concept map within a local terminology
      Get(s"/terminology/${localTerminology1.id}/conceptmap/${conceptMap1.id}/content") ~> route ~> check {
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
    "create a code system within a local terminology" in {
      // create a code system
      Post(s"/terminology/${localTerminology1.id}/codesystem", HttpEntity(ContentTypes.`application/json`, writePretty(codeSystem1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that the returned code system includes the id
        val codeSystem: TerminologyCodeSystem = JsonMethods.parse(responseAs[String]).extract[TerminologyCodeSystem]
        codeSystem.name shouldEqual "testCS"
        // validate that code system file is created
        new File(toFhirEngineConfig.contextPath + File.separatorChar +
          LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar +
          localTerminology1.name + File.separatorChar + codeSystem.name).exists() shouldEqual true
      }
      // create a second code system within same terminology
      Post(s"/terminology/${localTerminology1.id}/codesystem", HttpEntity(ContentTypes.`application/json`, writePretty(codeSystem2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that the returned code system includes the id
        val codeSystem: TerminologyCodeSystem = JsonMethods.parse(responseAs[String]).extract[TerminologyCodeSystem]
        codeSystem.name shouldEqual "testCS2"
        // validate that code system file is created
        new File(toFhirEngineConfig.contextPath + File.separatorChar +
          LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar +
          localTerminology1.name + File.separatorChar + codeSystem.name).exists() shouldEqual true
      }
    }

    "get code systems within a local terminology" in {
      // get a code system list within a local terminology
      Get(s"/terminology/${localTerminology1.id}/codesystem") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns one code systems
        val codeSystems: Seq[TerminologyCodeSystem] = JsonMethods.parse(responseAs[String]).extract[Seq[TerminologyCodeSystem]]
        codeSystems.length shouldEqual 2
      }
    }

    "get a code system within a local terminology" in {
      // get a code system within a local terminology
      Get(s"/terminology/${localTerminology1.id}/codesystem/${codeSystem1.id}").addHeader(RawHeader("Content-Type", "application/json")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns the code system
        val codeSystem: TerminologyCodeSystem = JsonMethods.parse(responseAs[String]).extract[TerminologyCodeSystem]
        codeSystem.name shouldEqual codeSystem1.name
      }
    }

    "put a code system within a local terminology" in {
      // update a code system within a local terminology
      Put(s"/terminology/${localTerminology1.id}/codesystem/${codeSystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(codeSystem1.copy(name = "testCSUpdated")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned code system includes the update
        val codeSystem: TerminologyCodeSystem = JsonMethods.parse(responseAs[String]).extract[TerminologyCodeSystem]
        codeSystem.name shouldEqual "testCSUpdated"
        // validate that code system file is updated
        new File(toFhirEngineConfig.contextPath + File.separatorChar +
          LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar +
          localTerminology1.name + File.separatorChar + codeSystem.name).exists() shouldEqual true

        // validate that old code system file is deleted
        new File(toFhirEngineConfig.contextPath + File.separatorChar +
          LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar +
          localTerminology1.name + File.separatorChar + codeSystem1.name).exists() shouldEqual false
        codeSystem1 = codeSystem
      }
    }

    "delete a code system within a local terminology" in {
      // delete a code system within a local terminology
      Delete(s"/terminology/${localTerminology1.id}/codesystem/${codeSystem2.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that code system file is deleted
        new File(toFhirEngineConfig.contextPath + File.separatorChar +
          LocalTerminologyFolderRepository.TERMINOLOGY_FOLDER + File.separatorChar +
          localTerminology1.name + File.separatorChar + codeSystem2.name).exists() shouldEqual false
      }
      // delete a non-existent code system within a local terminology
      Delete(s"/terminology/${localTerminology1.id}/codesystem/${codeSystem2.id}") ~> route ~> check {
        status should not equal StatusCodes.OK
      }
    }

    "upload a csv content as a code system within a local terminology" in {
      // get file from resources
      val file: File = FileOperations.getFileIfExists(getClass.getResource("/sample-code-system.csv").getPath)
      val fileData = Multipart.FormData.BodyPart.fromPath("attachment", ContentTypes.`text/plain(UTF-8)`, file.toPath)
      val formData = Multipart.FormData(fileData)
      // save a csv file as a concept map within a local terminology
      Post(s"/terminology/${localTerminology1.id}/codesystem/${codeSystem1.id}/content", formData.toEntity()) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "OK"
      }
    }

    "download csv content of a code system within a local terminology" in {
      // download csv content of a code system within a local terminology
      Get(s"/terminology/${localTerminology1.id}/codesystem/${codeSystem1.id}/content") ~> route ~> check {
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
