package io.tofhir.server.project

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import io.onfhir.util.JsonFormatter.formats
import io.tofhir.common.model.{DataTypeWithProfiles, SchemaDefinition, SimpleStructureDefinition}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.util.TestUtil
import org.json4s.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

class SchemaEndpointTest extends BaseEndpointTest {
  // first schema schema to be created
  val schema1: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema", `type` = "ty", name = "name", rootDefinition = None, fieldDefinitions = None)
  // second schema to be created
  val schema2: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema2", `type` = "ty2", name = "name2", rootDefinition = None, fieldDefinitions = None)
  // third schema to be created
  // it includes two elements:
  //  - element-with-definition => An element having a definition
  //  - element-with-no-definition => An element having no definition
  val schema3: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema3", `type` = "ty3", name = "name3", rootDefinition = None, fieldDefinitions = Some(
    Seq(
      SimpleStructureDefinition(id = "element-with-definition",
        path = "ty3.element-with-definition", dataTypes = Some(Seq(DataTypeWithProfiles(dataType = "canonical", profiles = Some(Seq("http://hl7.org/fhir/StructureDefinition/canonical"))))), isPrimitive = true,
        isChoiceRoot = false, isArray = false, minCardinality = 0, maxCardinality = None,
        boundToValueSet = None, isValueSetBindingRequired = None, referencableProfiles = None, constraintDefinitions = None, sliceDefinition = None,
        sliceName = None, fixedValue = None, patternValue = None, referringTo = None, short = Some("element-with-definition"), definition = Some("element definition"), comment = None, elements = None),
      SimpleStructureDefinition(id = "element-with-no-definition",
        path = "ty3.element-with-no-definition", dataTypes = Some(Seq(DataTypeWithProfiles(dataType = "canonical", profiles = Some(Seq("http://hl7.org/fhir/StructureDefinition/canonical"))))), isPrimitive = true,
        isChoiceRoot = false, isArray = false, minCardinality = 0, maxCardinality = None,
        boundToValueSet = None, isValueSetBindingRequired = None, referencableProfiles = None, constraintDefinitions = None, sliceDefinition = None,
        sliceName = None, fixedValue = None, patternValue = None, referringTo = None, short = Some("element-with-no-definition"), definition = None, comment = None, elements = None)
    )
  ))

  "The service" should {

    "create a schema within project" in {
      // create the first schema
      Post(s"/tofhir/projects/${projectId}/schemas", HttpEntity(ContentTypes.`application/json`, writePretty(schema1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that schema metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "schemas").asInstanceOf[JArray].arr.length shouldEqual 1
        // check schema folder is created
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, projectId, schema1.id).toFile.exists()
      }
      // create the second schema
      Post(s"/tofhir/projects/${projectId}/schemas", HttpEntity(ContentTypes.`application/json`, writePretty(schema2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that schema metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "schemas").asInstanceOf[JArray].arr.length shouldEqual 2
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, projectId, schema2.id).toFile.exists()
      }
    }

    "get all schemas in a project" in {
      // get all schemas within a project
      Get(s"/tofhir/projects/${projectId}/schemas") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two schemas
        val schemas: Seq[SchemaDefinition] = JsonMethods.parse(responseAs[String]).extract[Seq[SchemaDefinition]]
        schemas.length shouldEqual 2
      }
    }

    "get a schema in a project" in {
      // get a schema
      Get(s"/tofhir/projects/${projectId}/schemas/${schema1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual schema1.url
        schema.`type` shouldEqual schema1.`type`
      }
      // get a schema with invalid id
      Get(s"/tofhir/projects/${projectId}/schemas/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "get a schema by url in a project" in {
      Get(s"/tofhir/projects/${projectId}/schemas?url=${schema1.url}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual schema1.url
        schema.name shouldEqual schema1.name
      }
    }

    "update a schema in a project" in {
      // update a schema
      Put(s"/tofhir/projects/${projectId}/schemas/${schema1.id}", HttpEntity(ContentTypes.`application/json`, writePretty(schema1.copy(url = "https://example.com/fhir/StructureDefinition/schema3")))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that the returned schema includes the update
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual "https://example.com/fhir/StructureDefinition/schema3"
        // validate that schema metadata is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (
          (projects.arr.find(p => (p \ "id").extract[String] == projectId)
            .get \ "schemas").asInstanceOf[JArray].arr
            .find(m => (m \ "id").extract[String].contentEquals(schema1.id)).get \ "url"
          )
          .extract[String] shouldEqual "https://example.com/fhir/StructureDefinition/schema3"
      }
      // update a schema with invalid id
      Put(s"/tofhir/projects/${projectId}/schemas/123123", HttpEntity(ContentTypes.`application/json`, writePretty(schema2))) ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "delete a schema from a project" in {
      // delete a schema
      Delete(s"/tofhir/projects/${projectId}/schemas/${schema1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that schema metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "schemas").asInstanceOf[JArray].arr.length shouldEqual 1
        // check schema folder is deleted
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, projectId, schema1.id).toFile.exists() shouldEqual false
      }
      // delete a schema with invalid id
      Delete(s"/tofhir/projects/${projectId}/schemas/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "create a schema having some elements with/without description" in {
      // create the third schema
      Post(s"/tofhir/projects/${projectId}/schemas", HttpEntity(ContentTypes.`application/json`, writePretty(schema3))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
      }
      // retrieve the third schema
      Get(s"/tofhir/projects/${projectId}/schemas?url=${schema3.url}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual schema3.url
        schema.name shouldEqual schema3.name
        val fieldDefinitions = schema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 2
        fieldDefinitions.head.definition.get shouldEqual "element definition"
        fieldDefinitions.last.definition.nonEmpty shouldEqual false
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
