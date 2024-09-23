package io.tofhir.server.endpoint

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import io.tofhir.engine.data.write.FileSystemWriter.SinkContentTypes
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.util.FileOperations
import io.tofhir.engine.model.{FhirMapping, FhirMappingContextDefinition, FhirMappingJob, FhirMappingTask, FhirSinkSettings, FileSystemSinkSettings}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.util.TestUtil
import org.json4s.JArray
import org.json4s.jackson.JsonMethods
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.server.endpoint.{JobEndpoint, MappingEndpoint, ProjectEndpoint}
import org.json4s.jackson.Serialization.writePretty


class MappingEndpointTest extends BaseEndpointTest {

  // first mapping to be created
  val mapping1: FhirMapping = FhirMapping(id = "mapping1", url = "http://example.com/mapping1", name = "mapping1", source = Seq.empty,
    context = Map("practitionerConceptMap" -> FhirMappingContextDefinition(category = "concept-map", url = Some("$CONTEXT_REPO/pilot1/practitioner-concept-map.csv"),value = None)), mapping = Seq.empty)
  // second mapping to be created
  val mapping2: FhirMapping = FhirMapping(id = "mapping2", url = "http://example.com/mapping2", name = "mapping2", source = Seq.empty, context = Map.empty, mapping = Seq.empty)
  // job using mapping2
  val sinkSettings: FhirSinkSettings = FileSystemSinkSettings(path = "http://example.com/fhir", contentType = SinkContentTypes.CSV)
  val job: FhirMappingJob = FhirMappingJob(
    name = Some("mappingJob"),
    sourceSettings = Map.empty,
    sinkSettings = sinkSettings,
    mappings = Seq(FhirMappingTask(name = mapping2.id, mappingRef = mapping2.url, sourceBinding = Map.empty)))

  "The service" should {

    "create a mapping within project" in {
      // create the first mapping
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}", HttpEntity(ContentTypes.`application/json`, writePretty(mapping1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappings").asInstanceOf[JArray].arr.length shouldEqual 1
        // check mapping folder is created
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, projectId, s"${mapping1.id}${FileExtensions.JSON}").toFile should exist
      }
      // create the second mapping
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}", HttpEntity(ContentTypes.`application/json`, writePretty(mapping2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappings").asInstanceOf[JArray].arr.length shouldEqual 2
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, projectId, s"${mapping2.id}${FileExtensions.JSON}").toFile should exist
      }
    }

    "get all mappings in a project" in {
      // get all mappings within a project
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two mappings
        val mappings: Seq[FhirMapping] = JsonMethods.parse(responseAs[String]).extract[Seq[FhirMapping]]
        mappings.length shouldEqual 2
      }
    }

    "get a mapping in a project" in {
      // get a mapping
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}/${mapping1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved mapping
        val mapping: FhirMapping = JsonMethods.parse(responseAs[String]).extract[FhirMapping]
        mapping.url shouldEqual mapping1.url
        mapping.name shouldEqual mapping1.name
        // validate that the url of concept map is preserved
        mapping.context.size shouldEqual 1
        mapping.context("practitionerConceptMap").url.get shouldEqual "$CONTEXT_REPO/pilot1/practitioner-concept-map.csv"
      }
      // get a mapping with invalid id
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "update a mapping in a project" in {
      // update a mapping
      Put(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}/${mapping1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(mapping1.copy(url = "http://example.com/mapping3")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned mapping includes the update
        val mapping: FhirMapping = JsonMethods.parse(responseAs[String]).extract[FhirMapping]
        mapping.url shouldEqual "http://example.com/mapping3"
        // validate that mapping metadata is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (
          (projects.arr.find(p => (p \ "id").extract[String] == projectId)
            .get \ "mappings").asInstanceOf[JArray].arr
            .find(m => (m \ "id").extract[String].contentEquals(mapping1.id)).get \ "url"
          )
          .extract[String] shouldEqual "http://example.com/mapping3"
      }
      // update a mapping with invalid id
      Put(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}/123123", HttpEntity(ContentTypes.`application/json`, writePretty(mapping1.copy(id = "123123")))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
      // update a mapping with existing url
      Put(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}/${mapping1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(mapping1.copy(url = mapping2.url)))) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
      }
    }

    "delete a mapping from a project" in {
      // delete a mapping
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}/${mapping1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that mapping metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId)
          .get \ "mappings").asInstanceOf[JArray].arr.length shouldEqual 1
        // check mapping folder is deleted
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, projectId, s"${mapping1.id}${FileExtensions.JSON}").toFile shouldNot exist
      }
      // delete a mapping with invalid id
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "cannot delete a mapping from a project if it is referenced by some jobs" in {
      // create a job using mapping1
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${JobEndpoint.SEGMENT_JOB}", HttpEntity(ContentTypes.`application/json`, writePretty(job))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // check job folder is created
        FileUtils.getPath(toFhirEngineConfig.jobRepositoryFolderPath, projectId, s"${job.id}${FileExtensions.JSON}").toFile should exist
      }
      // delete a mapping
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}/${mapping2.id}") ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
      }
    }

    "cannot create a mapping with the existing ID or URL" in {
      // create the mapping with existing id
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}", HttpEntity(ContentTypes.`application/json`, writePretty(mapping2))) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
      }
      // create the mapping with existing url
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}", HttpEntity(ContentTypes.`application/json`, writePretty(mapping2.copy(id = "id-updated")))) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
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
