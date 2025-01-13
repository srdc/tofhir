package io.tofhir.server.endpoint

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import io.tofhir.engine.data.write.FileSystemWriter.SinkContentTypes
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.model.TerminologySystem
import io.tofhir.server.model.TerminologySystem.{TerminologyCodeSystem, TerminologyConceptMap}
import io.tofhir.server.repository.terminology.TerminologySystemFolderRepository.getTerminologySystemsJsonPath
import io.tofhir.server.util.{FileOperations, TestUtil}
import org.json4s.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

import java.io.File

class TerminologyServiceManagerEndpointTest extends BaseEndpointTest {
  var conceptMap1: TerminologyConceptMap = ConceptMapFile(name = "testCM", conceptMapUrl = "", sourceValueSetUrl = "", targetValueSetUrl = "")
  var conceptMap2: TerminologyConceptMap = ConceptMapFile(name = "testCM2", conceptMapUrl = "", sourceValueSetUrl = "", targetValueSetUrl = "")

  var codeSystem1: TerminologyCodeSystem = CodeSystemFile(name = "testCS", codeSystem = "")
  var codeSystem2: TerminologyCodeSystem = CodeSystemFile(name = "testCS2", codeSystem = "")

  var terminologySystem1: TerminologySystem = TerminologySystem(name = "testTerminology1", description = "example terminology 1", codeSystems = Seq.empty, conceptMaps = Seq.empty)

  val terminologySystem2: TerminologySystem = TerminologySystem(name = "testTerminology2", description = "example terminology 2", codeSystems = Seq.empty, conceptMaps = Seq.empty)

  // Create job for test update on job terminology
  val sinkSettings: FhirSinkSettings = FileSystemSinkSettings(path = "http://example.com/fhir", contentType = SinkContentTypes.CSV)
  val terminologyServiceSettings: TerminologyServiceSettings = LocalFhirTerminologyServiceSettings(s"/${toFhirEngineConfig.terminologySystemFolderPath}/${terminologySystem1.id}", codeSystemFiles = Seq.empty, conceptMapFiles = Seq.empty)
  val jobTest: FhirMappingJob = FhirMappingJob(name = Some("mappingJobTest"), sourceSettings = Map.empty, sinkSettings = sinkSettings, terminologyServiceSettings = Some(terminologyServiceSettings), mappings = Seq.empty, dataProcessingSettings = DataProcessingSettings())

  "The terminology service" should {
    "create a terminology system" in {
      // create the first terminology
      Post(s"/${webServerConfig.baseUri}/terminologies", HttpEntity(ContentTypes.`application/json`, writePretty(terminologySystem1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        val terminologySystem: TerminologySystem = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        // set the created terminology
        terminologySystem1 = terminologySystem
        // validate that a folder is created for the terminology
        FileUtils.getPath(toFhirEngineConfig.terminologySystemFolderPath, terminologySystem.id).toFile should exist
        // validate that terminologies metadata file is updated
        val terminologySystems
        = FileOperations.readJsonContent[TerminologySystem](FileUtils.getPath(getTerminologySystemsJsonPath(toFhirEngineConfig.terminologySystemFolderPath)).toFile)
        terminologySystems
          .length shouldEqual 1
      }
      // create the second terminology
      Post(s"/${webServerConfig.baseUri}/terminologies", HttpEntity(ContentTypes.`application/json`, writePretty(terminologySystem2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that terminologies metadata file is updated
        val terminologySystems
        = FileOperations.readJsonContent[TerminologySystem](FileUtils.getPath(getTerminologySystemsJsonPath(toFhirEngineConfig.terminologySystemFolderPath)).toFile)
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

      // try to get a non-existent terminology system
      Get(s"/${webServerConfig.baseUri}/terminologies/Non-existent") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
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
        = FileOperations.readJsonContent[TerminologySystem](FileUtils.getPath(getTerminologySystemsJsonPath(toFhirEngineConfig.terminologySystemFolderPath)).toFile)
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
        FileUtils.getPath(toFhirEngineConfig.terminologySystemFolderPath, terminologySystem2.id).toFile shouldNot exist
        // validate that terminology metadata file is updated
        val terminologySystems
        = FileOperations.readJsonContent[TerminologySystem](FileUtils.getPath(getTerminologySystemsJsonPath(toFhirEngineConfig.terminologySystemFolderPath)).toFile)
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
    "create a job and reference to a terminology system" in {
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}", HttpEntity(ContentTypes.`application/json`, writePretty(jobTest))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that job metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappingJobs").asInstanceOf[JArray].arr.length shouldEqual 1
        // check job folder is created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${jobTest.id}${FileExtensions.JSON}").toFile should exist
      }
    }

    // add a concept map to the terminology system object
    var updatedTerminology: TerminologySystem = terminologySystem1.copy(conceptMaps = terminologySystem1.conceptMaps :+ conceptMap1)

    "create a concept map within a terminology system" in {
      // create a concept map
      Put(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(updatedTerminology))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned concept map included in the terminologySystem
        updatedTerminology = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        updatedTerminology.conceptMaps.length shouldEqual 1
        updatedTerminology.conceptMaps.last.name shouldEqual "testCM"
        // validate that concept map file is created
        FileUtils.getPath(toFhirEngineConfig.terminologySystemFolderPath, terminologySystem1.id, updatedTerminology.conceptMaps.last.id).toFile should exist
      }
      // add a concept map to the terminology system object
      updatedTerminology = updatedTerminology.copy(conceptMaps = updatedTerminology.conceptMaps :+ conceptMap2)
      // create a second concept map within same terminology
      Put(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(updatedTerminology))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned concept map included in the terminologySystem
        updatedTerminology = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        updatedTerminology.conceptMaps.length shouldEqual 2
        updatedTerminology.conceptMaps.head.name shouldEqual "testCM"
        updatedTerminology.conceptMaps.last.name shouldEqual "testCM2"
        // validate that concept map file is created
        FileUtils.getPath(toFhirEngineConfig.terminologySystemFolderPath, terminologySystem1.id, updatedTerminology.conceptMaps.last.id).toFile should exist
      }
    }

    "get concept maps within a terminology system" in {
      // get a concept map list within a terminology system
      Get(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}/${ConceptMapEndpoint.SEGMENT_CONCEPT_MAPS}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns one concept maps
        val conceptMaps: Seq[TerminologyConceptMap] = JsonMethods.parse(responseAs[String]).extract[Seq[TerminologyConceptMap]]
        conceptMaps.length shouldEqual 2
      }
    }

    "get a concept map within a terminology system" in {
      // get a concept map within a terminology system
      Get(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}/${ConceptMapEndpoint.SEGMENT_CONCEPT_MAPS}/${conceptMap1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns the concept map
        val conceptMap: TerminologyConceptMap = JsonMethods.parse(responseAs[String]).extract[TerminologyConceptMap]
        conceptMap.name shouldEqual conceptMap1.name
      }

      // try to get a non-existent concept map within a terminology system
      Get(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}/${ConceptMapEndpoint.SEGMENT_CONCEPT_MAPS}/Non-existent") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "get the job in a project to verify terminology system is do not updated with new conceptMaps" in {
      // get the job
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}/${jobTest.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved job includes new conceptMaps
        val job: FhirMappingJob = JsonMethods.parse(responseAs[String]).extract[FhirMappingJob]
        job.id shouldEqual jobTest.id
        job.terminologyServiceSettings.get.asInstanceOf[LocalFhirTerminologyServiceSettings].conceptMapFiles.length shouldEqual 0
      }
    }

    "put a concept map within a terminology system" in {
      // update concept maps in the terminology system object
      updatedTerminology = updatedTerminology.copy(conceptMaps = updatedTerminology.conceptMaps.map(conceptMap => conceptMap.copy(name = conceptMap.name + "Updated")))
      // update concept maps within same terminology
      Put(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(updatedTerminology))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the updated conceptMaps included in the terminologySystem
        updatedTerminology = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        updatedTerminology.conceptMaps.length shouldEqual 2
        updatedTerminology.conceptMaps.head.name shouldEqual "testCMUpdated"
        updatedTerminology.conceptMaps.last.name shouldEqual "testCM2Updated"
        conceptMap1 = updatedTerminology.conceptMaps.head
      }
    }


    "delete a concept map within a terminology system" in {
      // delete a concept map in the terminology system object
      updatedTerminology = updatedTerminology.copy(conceptMaps = Seq(updatedTerminology.conceptMaps.head))
      // delete the concept map within same terminology
      Put(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(updatedTerminology))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned only first concept map in the terminologySystem
        updatedTerminology = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        updatedTerminology.conceptMaps.length shouldEqual 1
        updatedTerminology.conceptMaps.head.name shouldEqual "testCMUpdated"
        // validate that concept map file is deleted
        FileUtils.getPath(toFhirEngineConfig.terminologySystemFolderPath, terminologySystem1.id, conceptMap2.id).toFile shouldNot exist
      }
    }


    "upload a csv content as a concept map within a terminology system" in {
      // get file from resources
      val file: File = FileOperations.getFileIfExists(getClass.getResource("/test-terminology-service/sample-concept-map.csv").getPath)
      val fileData = Multipart.FormData.BodyPart.fromPath("attachment", ContentTypes.`text/plain(UTF-8)`, file.toPath)
      val formData = Multipart.FormData(fileData)
      // save a csv file as a concept map within a terminology system
      Post(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}/${ConceptMapEndpoint.SEGMENT_CONCEPT_MAPS}/${conceptMap1.id}/${TerminologyServiceManagerEndpoint.SEGMENT_FILE}", formData.toEntity()) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "OK"
        Thread.sleep(5000)
      }
    }


    "download csv content of a concept map within a terminology system" in {
      // download csv content of a concept map within a terminology system
      Get(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}/${ConceptMapEndpoint.SEGMENT_CONCEPT_MAPS}/${conceptMap1.id}/${TerminologyServiceManagerEndpoint.SEGMENT_FILE}") ~> route ~> check {
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

    // add a code system to the terminology system object
    var updatedTerminology: TerminologySystem = terminologySystem1.copy(codeSystems = terminologySystem1.codeSystems :+ codeSystem1)

    "create a code system within a terminology system" in {
      // create a code system
      Put(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(updatedTerminology))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned code system included in the terminologySystem
        updatedTerminology = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        updatedTerminology.codeSystems.length shouldEqual 1
        updatedTerminology.codeSystems.last.name shouldEqual "testCS"
        // validate that code system file is created
        FileUtils.getPath(toFhirEngineConfig.terminologySystemFolderPath, terminologySystem1.id, updatedTerminology.codeSystems.last.id).toFile should exist
      }
      // add a code system to the terminology system object
      updatedTerminology = updatedTerminology.copy(codeSystems = updatedTerminology.codeSystems :+ codeSystem2)
      // create a second code system within same terminology
      Put(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(updatedTerminology))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned code system included in the terminologySystem
        updatedTerminology = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        updatedTerminology.codeSystems.length shouldEqual 2
        updatedTerminology.codeSystems.head.name shouldEqual "testCS"
        updatedTerminology.codeSystems.last.name shouldEqual "testCS2"
        // validate that code system file is created
        FileUtils.getPath(toFhirEngineConfig.terminologySystemFolderPath, terminologySystem1.id, updatedTerminology.codeSystems.last.id).toFile should exist
      }
    }

    "get code systems within a terminology system" in {
      // get a code system list within a terminology system
      Get(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}/${CodeSystemEndpoint.SEGMENT_CODE_SYSTEMS}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns one code systems
        val codeSystems: Seq[TerminologyCodeSystem] = JsonMethods.parse(responseAs[String]).extract[Seq[TerminologyCodeSystem]]
        codeSystems.length shouldEqual 2
      }
    }

    "get a code system within a terminology system" in {
      // get a code system within a terminology system
      Get(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}/${CodeSystemEndpoint.SEGMENT_CODE_SYSTEMS}/${codeSystem1.id}").addHeader(RawHeader("Content-Type", "application/json")) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns the code system
        val codeSystem: TerminologyCodeSystem = JsonMethods.parse(responseAs[String]).extract[TerminologyCodeSystem]
        codeSystem.name shouldEqual codeSystem1.name
      }

      // try to get a non-existent code system within a terminology system
      Get(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}/${CodeSystemEndpoint.SEGMENT_CODE_SYSTEMS}/Non-existent").addHeader(RawHeader("Content-Type", "application/json")) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "get the job in a project to verify terminology system is do not updated with new codeSystems" in {
      // get the job
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}/${jobTest.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved job includes new codeSystems
        val job: FhirMappingJob = JsonMethods.parse(responseAs[String]).extract[FhirMappingJob]
        job.id shouldEqual jobTest.id
        job.terminologyServiceSettings.get.asInstanceOf[LocalFhirTerminologyServiceSettings].codeSystemFiles.length shouldEqual 0
      }
    }

    "put a code system within a terminology system" in {
      // update code systems in the terminology system object
      updatedTerminology = updatedTerminology.copy(codeSystems = updatedTerminology.codeSystems.map(codeSystem => codeSystem.copy(name = codeSystem.name + "Updated")))
      // update code systems within same terminology
      Put(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(updatedTerminology))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the updated codeSystems included in the terminologySystem
        updatedTerminology = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        updatedTerminology.codeSystems.length shouldEqual 2
        updatedTerminology.codeSystems.head.name shouldEqual "testCSUpdated"
        updatedTerminology.codeSystems.last.name shouldEqual "testCS2Updated"
        codeSystem1 = updatedTerminology.codeSystems.head
      }
    }

    "delete a code system within a terminology system" in {
      // delete a code system in the terminology system object
      updatedTerminology = updatedTerminology.copy(codeSystems = Seq(updatedTerminology.codeSystems.head))
      // delete the code system within same terminology
      Put(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(updatedTerminology))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned only first code system in the terminologySystem
        updatedTerminology = JsonMethods.parse(responseAs[String]).extract[TerminologySystem]
        updatedTerminology.codeSystems.length shouldEqual 1
        updatedTerminology.codeSystems.head.name shouldEqual "testCSUpdated"
        // validate that code system file is deleted
        FileUtils.getPath(toFhirEngineConfig.terminologySystemFolderPath, terminologySystem1.id, codeSystem2.id).toFile shouldNot exist
      }
    }

    "upload a csv content as a code system within a terminology system" in {
      // get file from resources
      val file: File = FileOperations.getFileIfExists(getClass.getResource("/test-terminology-service/sample-code-system.csv").getPath)
      val fileData = Multipart.FormData.BodyPart.fromPath("attachment", ContentTypes.`text/plain(UTF-8)`, file.toPath)
      val formData = Multipart.FormData(fileData)
      // save a csv file as a concept map within a terminology system
      Post(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}/${CodeSystemEndpoint.SEGMENT_CODE_SYSTEMS}/${codeSystem1.id}/${TerminologyServiceManagerEndpoint.SEGMENT_FILE}", formData.toEntity()) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual "OK"
        Thread.sleep(5000)
      }
    }

    "download csv content of a code system within a terminology system" in {
      // download csv content of a code system within a terminology system
      Get(s"/${webServerConfig.baseUri}/${TerminologyServiceManagerEndpoint.SEGMENT_TERMINOLOGY}/${terminologySystem1.id}/${CodeSystemEndpoint.SEGMENT_CODE_SYSTEMS}/${codeSystem1.id}/${TerminologyServiceManagerEndpoint.SEGMENT_FILE}") ~> route ~> check {
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

  /**
   * Creates a project to be used in the tests
   * */
  override def beforeAll(): Unit = {
    super.beforeAll()
    this.createProject()
  }
}
