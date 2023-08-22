package io.tofhir.server.project

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.testkit.RouteTestTimeout
import io.tofhir.common.model.{DataTypeWithProfiles, SchemaDefinition, SimpleStructureDefinition}
import io.tofhir.engine.model.{FhirMapping, FhirMappingSource, SqlSource, SqlSourceSettings}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.model.InferTask
import io.tofhir.server.util.TestUtil
import org.json4s.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import java.sql.{Connection, DriverManager, Statement}

import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Using}
import scala.concurrent.duration._

class SchemaEndpointTest extends BaseEndpointTest {
  // database url for infer schema test
  val DATABASE_URL = "jdbc:h2:mem:inputDb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"
  // set 5 second timeout for test because infer schema test can take longer than 1 second
  implicit def default(implicit system: ActorSystem): RouteTestTimeout = RouteTestTimeout(5.seconds)
  // inferTask object for infer schema test
  val inferTask: InferTask = InferTask(name="test", sourceSettings = Map(
    "source" ->
      SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data", databaseUrl = DATABASE_URL, username = "", password = "")
  ), sourceContext = SqlSource(query = Some("select * from death"), preprocessSql = Some("select person_id, death_date, death_datetime, cause_source_value from test")))


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
  // fourth schema to be created
  // it includes two elements:
  //  - element-with-short => An element having a short
  //  - element-with-no-short => An element having no short
  val schema4: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema4", `type` = "ty4", name = "name4", rootDefinition = None, fieldDefinitions = Some(
    Seq(
      SimpleStructureDefinition(id = "element-with-short",
        path = "ty4.element-with-short", dataTypes = Some(Seq(DataTypeWithProfiles(dataType = "canonical", profiles = Some(Seq("http://hl7.org/fhir/StructureDefinition/canonical"))))), isPrimitive = true,
        isChoiceRoot = false, isArray = false, minCardinality = 0, maxCardinality = None,
        boundToValueSet = None, isValueSetBindingRequired = None, referencableProfiles = None, constraintDefinitions = None, sliceDefinition = None,
        sliceName = None, fixedValue = None, patternValue = None, referringTo = None, short = Some("element-with-short"), definition = None, comment = None, elements = None),
      SimpleStructureDefinition(id = "element-with-no-short",
        path = "ty4.element-with-no-short", dataTypes = Some(Seq(DataTypeWithProfiles(dataType = "canonical", profiles = Some(Seq("http://hl7.org/fhir/StructureDefinition/canonical"))))), isPrimitive = true,
        isChoiceRoot = false, isArray = false, minCardinality = 0, maxCardinality = None,
        boundToValueSet = None, isValueSetBindingRequired = None, referencableProfiles = None, constraintDefinitions = None, sliceDefinition = None,
        sliceName = None, fixedValue = None, patternValue = None, referringTo = None, short = None, definition = None, comment = None, elements = None)
    )
  ))

  // mapping using schema2
  val mapping: FhirMapping = FhirMapping(id = "mapping", url = "http://example.com/mapping", name = "mapping", source = Seq(FhirMappingSource(alias="test",url = "https://example.com/fhir/StructureDefinition/schema2")), context = Map.empty, mapping = Seq.empty)

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

    "cannot delete a schema from a project if it is referenced by some mappings" in {
      // create the mapping which makes use of schema2
      Post(s"/${webServerConfig.baseUri}/projects/${projectId}/mappings", HttpEntity(ContentTypes.`application/json`, writePretty(mapping))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappings").asInstanceOf[JArray].arr.length shouldEqual 1
        // check mapping folder is created
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, projectId, mapping.id).toFile.exists()
      }

      // delete schema2
      Delete(s"/tofhir/projects/${projectId}/schemas/${schema2.id}") ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
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

    "create a schema having some elements with/without short" in {
      // create the fourth schema
      Post(s"/tofhir/projects/${projectId}/schemas", HttpEntity(ContentTypes.`application/json`, writePretty(schema4))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
      }
      // retrieve the fourth schema
      Get(s"/tofhir/projects/${projectId}/schemas?url=${schema4.url}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual schema4.url
        schema.name shouldEqual schema4.name
        val fieldDefinitions = schema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 2
        fieldDefinitions.head.short.get shouldEqual "element-with-short"
        fieldDefinitions.last.short.nonEmpty shouldEqual false
      }
    }

    "Infer the schema and retrieve column types" in {
      // infer the schema
      Post(s"/tofhir/projects/${projectId}/schemas/infer", HttpEntity(ContentTypes.`application/json`, writePretty(inferTask))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate data types of schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        val fieldDefinitions = schema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 4
        fieldDefinitions.head.dataTypes.get.head.dataType shouldEqual "integer"
        fieldDefinitions(1).dataTypes.get.head.dataType shouldEqual "date"
        fieldDefinitions(2).dataTypes.get.head.dataType shouldEqual "dateTime"
        fieldDefinitions(3).dataTypes.get.head.dataType shouldEqual "string"
      }
    }
  }

  /**
   * Creates a project to be used in the tests. Moreover, it creates a SQL table to be used in the testing of infer schema functionality.
   * */
  override def beforeAll(): Unit = {
    super.beforeAll()
    val sql = readFileContent("/sql/sql-source-populate.sql")
    runSQL(sql)
    this.createProject()
  }

  /**
   * Drop tables after schema inferring.
   * */
  override def afterAll(): Unit = {
    val sql = readFileContent("/sql/sql-source-drop.sql")
    runSQL(sql)
    super.afterAll()
  }

  /**
   * Read sql file from file system
   * */
  private def readFileContent(fileName: String): String = {
    val source: BufferedSource = Source.fromInputStream(getClass.getResourceAsStream(fileName))
    try source.mkString finally source.close()
  }

  /**
   * Run SQL queries for setting up database
   * */
  private def runSQL(sql: String): Unit = {
    Using.Manager { use =>
      val con: Connection = use(DriverManager.getConnection(DATABASE_URL))
      val stm: Statement = use(con.createStatement)
      stm.execute(sql)
      con.close()
    } match {
      case Success(value) => value
      case Failure(e) => throw e
    }
  }
}
