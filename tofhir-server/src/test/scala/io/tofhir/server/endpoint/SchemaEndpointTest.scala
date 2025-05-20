package io.tofhir.server.endpoint

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{ContentType, ContentTypes, HttpEntity, MediaTypes, Multipart, StatusCodes}
import akka.http.scaladsl.testkit.RouteTestTimeout
import io.onfhir.api.client.FhirBatchTransactionRequestBuilder
import io.onfhir.api.{FHIR_DATA_TYPES, FHIR_FOUNDATION_RESOURCES, Resource}
import io.tofhir.OnFhirTestContainer
import io.onfhir.definitions.common.model.{DataTypeWithProfiles, SchemaDefinition, SimpleStructureDefinition}
import io.tofhir.engine.model._
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.BaseEndpointTest
import io.tofhir.server.endpoint.SchemaDefinitionEndpoint.{QUERY_PARAM_TYPE, QUERY_PARAM_URL}
import io.tofhir.server.model.{ImportSchemaSettings, InferTask}
import io.tofhir.server.util.{FileOperations, TestUtil}
import org.json4s.JArray
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.Serialization.writePretty

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, Statement}
import scala.concurrent.duration._
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Using}

class SchemaEndpointTest extends BaseEndpointTest with OnFhirTestContainer {
  // database url for infer schema test
  val DATABASE_URL = "jdbc:h2:mem:inputDb;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE"

  // set 5 second timeout for test because infer schema test can take longer than 1 second
  implicit def default(implicit system: ActorSystem): RouteTestTimeout = RouteTestTimeout(5.seconds)

  // inferTask object for infer schema test
  val inferTaskWithTableName: InferTask = InferTask(name = "test", mappingJobSourceSettings = Map(
    "source" ->
      SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data", databaseUrl = DATABASE_URL, username = "", password = "")
  ), sourceBinding = SqlSource(tableName = Some("death")))

  val q =
    """SELECT
      |    p.person_id,
      |    p.name,
      |    p.surname,
      |    p.date_of_birth,
      |    d.death_date,
      |    d.death_datetime
      |FROM person p, death d WHERE p.person_id = d.person_id""".stripMargin

  val inferTaskWithSqlQuery: InferTask = InferTask(name = "test", mappingJobSourceSettings = Map(
    "source" ->
      SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data", databaseUrl = DATABASE_URL, username = "", password = "")
  ), sourceBinding = SqlSource(query = Some(q)))

  val inferTaskWithPreprocessSql: InferTask = InferTask(name = "test", mappingJobSourceSettings = Map(
    "source" ->
      SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data", databaseUrl = DATABASE_URL, username = "", password = "")
  ), sourceBinding = SqlSource(query = Some("select * from death"), preprocessSql = Some("select person_id, death_date, death_datetime, cause_source_value from test")))


  // first schema to be created
  val schema1: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema", version = SchemaDefinition.VERSION_LATEST, `type` = "Ty", name = "name", description = Some("description"), rootDefinition = None, fieldDefinitions = None)
  // second schema to be created
  val schema2: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema2", version = SchemaDefinition.VERSION_LATEST, `type` = "Ty2", name = "name2", description = Some("description2"), rootDefinition = None, fieldDefinitions = None)
  val schema2_2: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema2", version = "0.9.12", `type` = "Ty2", name = "name2", description = Some("old-version-description2"), rootDefinition = None, fieldDefinitions = None)
  // third schema to be created
  // it includes two elements:
  //  - element-with-definition => An element having a definition
  //  - element-with-no-definition => An element having no definition
  val schema3: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema3", version = SchemaDefinition.VERSION_LATEST, `type` = "Ty3", name = "name3", description = Some("description3"), rootDefinition = None, fieldDefinitions = Some(
    Seq(
      SimpleStructureDefinition(id = "element-with-definition",
        path = "Ty3.element-with-definition", dataTypes = Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.CANONICAL, profiles = Some(Seq("http://hl7.org/fhir/StructureDefinition/canonical"))))), isPrimitive = true,
        isChoiceRoot = false, isArray = false, minCardinality = 0, maxCardinality = None,
        boundToValueSet = None, isValueSetBindingRequired = None, referencableProfiles = None, constraintDefinitions = None, sliceDefinition = None,
        sliceName = None, fixedValue = None, patternValue = None, referringTo = None, short = Some("element-with-definition"), definition = Some("element definition"), comment = None, elements = None),
      SimpleStructureDefinition(id = "element-with-no-definition",
        path = "Ty3.element-with-no-definition", dataTypes = Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.CANONICAL, profiles = Some(Seq("http://hl7.org/fhir/StructureDefinition/canonical"))))), isPrimitive = true,
        isChoiceRoot = false, isArray = false, minCardinality = 0, maxCardinality = None,
        boundToValueSet = None, isValueSetBindingRequired = None, referencableProfiles = None, constraintDefinitions = None, sliceDefinition = None,
        sliceName = None, fixedValue = None, patternValue = None, referringTo = None, short = Some("element-with-no-definition"), definition = None, comment = None, elements = None)
    )
  ))
  // fourth schema to be created
  // it includes two elements:
  //  - element-with-short => An element having a short
  //  - element-with-no-short => An element having no short
  val schema4: SchemaDefinition = SchemaDefinition(url = "https://example.com/fhir/StructureDefinition/schema4", version = SchemaDefinition.VERSION_LATEST, `type` = "Ty4", name = "name4", description = Some("description4"), rootDefinition = None, fieldDefinitions = Some(
    Seq(
      SimpleStructureDefinition(id = "element-with-short",
        path = "Ty4.element-with-short", dataTypes = Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.CANONICAL, profiles = Some(Seq("http://hl7.org/fhir/StructureDefinition/canonical"))))), isPrimitive = true,
        isChoiceRoot = false, isArray = false, minCardinality = 0, maxCardinality = None,
        boundToValueSet = None, isValueSetBindingRequired = None, referencableProfiles = None, constraintDefinitions = None, sliceDefinition = None,
        sliceName = None, fixedValue = None, patternValue = None, referringTo = None, short = Some("element-with-short"), definition = None, comment = None, elements = None),
      SimpleStructureDefinition(id = "element-with-no-short",
        path = "Ty4.element-with-no-short", dataTypes = Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.CANONICAL, profiles = Some(Seq("http://hl7.org/fhir/StructureDefinition/canonical"))))), isPrimitive = true,
        isChoiceRoot = false, isArray = false, minCardinality = 0, maxCardinality = None,
        boundToValueSet = None, isValueSetBindingRequired = None, referencableProfiles = None, constraintDefinitions = None, sliceDefinition = None,
        sliceName = None, fixedValue = None, patternValue = None, referringTo = None, short = None, definition = None, comment = None, elements = None)
    )
  ))
  // fifth schema with the same url and version as the second schema (but different id's)
  val schema5: SchemaDefinition = SchemaDefinition(id = "schema5", url = "https://example.com/fhir/StructureDefinition/schema2", version = SchemaDefinition.VERSION_LATEST, `type` = "Ty5", name = "name5", description = Some("description5"), rootDefinition = None, fieldDefinitions = None)

  // mapping using schema2
  val mapping: FhirMapping = FhirMapping(id = "mapping", url = "http://example.com/mapping", name = "mapping", source = Seq(FhirMappingSource(alias = "test", url = "https://example.com/fhir/StructureDefinition/schema2")), context = Map.empty, mapping = Seq.empty)

  "The service" should {

    "create a schema within project" in {
      // create the first schema
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(schema1))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that schema metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "schemas").asInstanceOf[JArray].arr.length shouldEqual 1
        // check schema folder is created
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, projectId, s"${schema1.id}${FileExtensions.JSON}").toFile should exist
      }
      // create the second schema
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(schema2))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that schema metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "schemas").asInstanceOf[JArray].arr.length shouldEqual 2
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, projectId, s"${schema2.id}${FileExtensions.JSON}").toFile should exist
      }
    }

    "get all schemas in a project" in {
      // get all schemas within a project
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate that it returns two schemas
        val schemas: Seq[SchemaDefinition] = JsonMethods.parse(responseAs[String]).extract[Seq[SchemaDefinition]]
        schemas.length shouldEqual 2
      }
    }

    "get a schema in a project" in {
      // get a schema
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${schema1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual schema1.url
        schema.`type` shouldEqual schema1.`type`
      }
      // get a schema with invalid id
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "get a schema by url in a project" in {
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}?url=${schema1.url}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual schema1.url
        schema.name shouldEqual schema1.name
      }
    }

    "update a schema in a project" in {
      // update a schema
      Put(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${schema1.id}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(schema1.copy(url = "https://example.com/fhir/StructureDefinition/schema3", name = schema3.name)))) ~> route ~> check {
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
      Put(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/123123",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(schema2))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
      }
    }

    "delete a schema from a project" in {
      // delete a schema
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${schema1.id}") ~> route ~> check {
        status shouldEqual StatusCodes.NoContent
        // validate that schema metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "schemas").asInstanceOf[JArray].arr.length shouldEqual 1
        // check schema folder is deleted
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, projectId, s"${schema1.id}${FileExtensions.JSON}").toFile shouldNot exist
      }
      // delete a schema with invalid id
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/123123") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "cannot delete a schema from a project if it is referenced by some mappings" in {
      // create the mapping which makes use of schema2
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${MappingEndpoint.SEGMENT_MAPPINGS}", HttpEntity(ContentTypes.`application/json`, writePretty(mapping))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
        // validate that mapping metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "mappings").asInstanceOf[JArray].arr.length shouldEqual 1
        // check mapping folder is created
        FileUtils.getPath(toFhirEngineConfig.mappingRepositoryFolderPath, projectId, s"${mapping.id}${FileExtensions.JSON}").toFile should exist
      }

      // delete schema2
      Delete(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${schema2.id}") ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
      }
    }

    "create a schema having some elements with/without description" in {
      // create the third schema
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}", HttpEntity(ContentTypes.`application/json`, writePretty(schema3))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
      }
      // retrieve the third schema
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}?url=${schema3.url}") ~> route ~> check {
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
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(schema4))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
      }
      // retrieve the fourth schema
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}?url=${schema4.url}") ~> route ~> check {
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

    "infer the schema on a given table name and retrieve column types" in {
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_INFER}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(inferTaskWithTableName))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        val fieldDefinitions = schema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 7
        fieldDefinitions.head.dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.INTEGER
        fieldDefinitions(1).dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.DATE
        fieldDefinitions(1).minCardinality shouldEqual 1
        fieldDefinitions(2).dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.DATETIME
        fieldDefinitions(2).minCardinality shouldEqual 0
        fieldDefinitions(5).dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.STRING
      }
    }

    "infer the schema on a given SQL query and retrieve column types" in {
      // infer the schema
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_INFER}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(inferTaskWithSqlQuery))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        val fieldDefinitions = schema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 6
        fieldDefinitions.head.dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.INTEGER
        fieldDefinitions(1).dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.STRING
        fieldDefinitions(1).minCardinality shouldEqual 0
        fieldDefinitions(3).dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.DATE
        fieldDefinitions(3).minCardinality shouldEqual 1
        fieldDefinitions(5).dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.DATETIME
      }
    }

    "infer the schema on a given SQL query with a preprocess SQL and retrieve column types" in {
      // infer the schema
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_INFER}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(inferTaskWithPreprocessSql))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate data types of schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        val fieldDefinitions = schema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 4
        fieldDefinitions.head.dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.INTEGER
        fieldDefinitions(1).dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.DATE
        fieldDefinitions(2).dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.DATETIME
        fieldDefinitions(3).dataTypes.get.head.dataType shouldEqual FHIR_DATA_TYPES.STRING
      }
    }

    "create schemas having the same URL but different versions" in {
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(schema5.copy(version = "some-version-other-than-latest")))) ~> route ~> check {
        // Expect a successful write
        status shouldEqual StatusCodes.Created
        // validate that schema metadata file is updated
        val projects: JArray = TestUtil.getProjectJsonFile(toFhirEngineConfig)
        (projects.arr.find(p => (p \ "id").extract[String] == projectId).get \ "schemas").asInstanceOf[JArray].arr.length shouldEqual 4
        FileUtils.getPath(toFhirEngineConfig.schemaRepositoryFolderPath, projectId, s"${schema5.id}${FileExtensions.JSON}").toFile should exist
      }
    }

    "cannot create a schema having the same id, url or name as another schema" in {
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(schema5))) ~> route ~> check {
        // Expect a conflict status because schema5 has the same url and version as the schema2
        status shouldEqual StatusCodes.Conflict
      }
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(schema5.copy(id = schema2.id)))) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
      }
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(schema5.copy(name = schema2.name)))) ~> route ~> check {
        status shouldEqual StatusCodes.Conflict
      }
    }

    "import REDCap data dictionary file" in {
      // get file from resources
      val file: File = FileOperations.getFileIfExists(getClass.getResource("/redcap/instrument.csv").getPath)
      val fileData = Multipart.FormData.BodyPart.fromPath("attachment", ContentTypes.`text/csv(UTF-8)`, file.toPath)
      val formData = Multipart.FormData(fileData)
      // the root URL of the schemas
      val definitionRootUrl = "http://test-schema"
      // the form field which keeps the unique identifier of REDCap records
      val recordIdField = "record_id"
      // the identifiers of schemas to be created. They are the form names given in the instrument.csv
      val firstSchemaId = "Test"
      val secondSchemaId = "Additional_information"

      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_REDCAP}?rootUrl=$definitionRootUrl&recordIdField=$recordIdField", formData.toEntity()) ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the schemas
        val schemas: Seq[SchemaDefinition] = JsonMethods.parse(responseAs[String]).extract[Seq[SchemaDefinition]]
        schemas.size shouldEqual 2
        // validate the first one
        val firstSchema = schemas.head
        firstSchema.url shouldEqual s"$definitionRootUrl/${FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION}/$firstSchemaId"
        var fieldDefinitions = firstSchema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 5
        // every field of a REDCap schema should be primitive
        fieldDefinitions.count(_.isPrimitive) shouldEqual 5

        // validate the second one
        val secondSchema = schemas.last
        secondSchema.url shouldEqual s"$definitionRootUrl/${FHIR_FOUNDATION_RESOURCES.FHIR_STRUCTURE_DEFINITION}/$secondSchemaId"
        fieldDefinitions = secondSchema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 4
        // 'record_id' field should be added automatically even if it does not exist in the form definition
        fieldDefinitions.count(definition => definition.id.contentEquals(recordIdField)) shouldEqual 1
      }
    }

    "create an HTTP response with bad request for file data sources with wrong file extension" in {
      val erroneousInferTask = inferTaskWithSqlQuery.copy(mappingJobSourceSettings = inferTaskWithSqlQuery.mappingJobSourceSettings.updated(
        "source", FileSystemSourceSettings(
          name = "test-db-source",
          sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data",
          dataFolderPath = "data-integration-suite/test-data"
        )),
        sourceBinding = FileSystemSource("WRONG.wrong", contentType = SourceContentTypes.CSV)
      )
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_INFER}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(erroneousInferTask))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/BadRequest")
      }
    }

    "create an HTTP response with bad request for data data sources with wrong file path" in {
      val erroneousInferTask = inferTaskWithSqlQuery.copy(mappingJobSourceSettings = inferTaskWithSqlQuery.mappingJobSourceSettings.updated(
        "source", FileSystemSourceSettings(
          name = "test-db-source",
          sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data",
          dataFolderPath = "data-integration-suite/test-data"
        )),
        sourceBinding = FileSystemSource("lab-results.csv", contentType = SourceContentTypes.CSV)
      )
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_INFER}",
        HttpEntity(ContentTypes.`application/json`,
          writePretty(erroneousInferTask))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/BadRequest")
      }
    }

    "create an HTTP response with bad request for wrong preprocess SQL string" in {
      val erroneousInferTask = inferTaskWithSqlQuery.copy(mappingJobSourceSettings = inferTaskWithSqlQuery.mappingJobSourceSettings.updated(
        "source",
        SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data", databaseUrl = DATABASE_URL, username = "", password = "")),
        sourceBinding = SqlSource(query = Some("Select * from death"), preprocessSql = Some("Wrong query string"))
      )
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_INFER}", HttpEntity(ContentTypes.`application/json`, writePretty(erroneousInferTask))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/BadRequest")
      }
    }

    "create an HTTP response with bad request for wrong user credentials to access the DB" in {
      val erroneousInferTask = inferTaskWithSqlQuery.copy(mappingJobSourceSettings = inferTaskWithSqlQuery.mappingJobSourceSettings.updated(
        "source", SqlSourceSettings(
          name = "test-db-source",
          sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data",
          databaseUrl = DATABASE_URL,
          username = "Wrong username",
          password = ""
        ))
      )
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_INFER}", HttpEntity(ContentTypes.`application/json`, writePretty(erroneousInferTask))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/BadRequest")
      }
    }

    "create an HTTP response with bad request for wrong database URL" in {
      val erroneousInferTask = inferTaskWithSqlQuery.copy(mappingJobSourceSettings = inferTaskWithSqlQuery.mappingJobSourceSettings.updated(
        "source", SqlSourceSettings(
          name = "test-db-source",
          sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data",
          databaseUrl = "WRONG DATABASE URL",
          username = "",
          password = ""
        ))
      )
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_INFER}", HttpEntity(ContentTypes.`application/json`, writePretty(erroneousInferTask))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/BadRequest")
      }
    }

    "create an HTTP response with bad request for erroneous SQL query" in {
      val erroneousInferTask = inferTaskWithSqlQuery.copy(mappingJobSourceSettings = inferTaskWithSqlQuery.mappingJobSourceSettings.updated(
        "source",
        SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data", databaseUrl = DATABASE_URL, username = "", password = "")),
        sourceBinding = SqlSource(query = Some("WRONG"))
      )
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_INFER}", HttpEntity(ContentTypes.`application/json`, writePretty(erroneousInferTask))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/BadRequest")
      }
    }

    "create an HTTP response with bad request for wrong column name in the SQL query" in {
      val erroneousInferTask = inferTaskWithSqlQuery.copy(mappingJobSourceSettings = inferTaskWithSqlQuery.mappingJobSourceSettings.updated(
        "source",
        SqlSourceSettings(name = "test-db-source", sourceUri = "https://aiccelerate.eu/data-integration-suite/test-data", databaseUrl = DATABASE_URL, username = "", password = "")),
        sourceBinding = SqlSource(query = Some("select WRONG from death"))
      )
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_INFER}", HttpEntity(ContentTypes.`application/json`, writePretty(erroneousInferTask))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[String]
        response should include("Type: https://tofhir.io/errors/BadRequest")
      }
    }

    "get structure definition of a schema to export" in {
      // get a schema structure definition by defining format parameter
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${schema3.id}?format=${SchemaFormats.STRUCTURE_DEFINITION}") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schemaResource: Resource = JsonMethods.parse(responseAs[String]).extract[Resource]
        // Create a map from resource json
        var schemaResourceMap: Map[String, Any] = Map.empty
        schemaResource.values.foreach(tuple => schemaResourceMap += tuple._1 -> tuple._2)
        // Validate some fields of the schema
        schemaResourceMap should contain allOf(
          "id" -> schema3.id,
          "url" -> schema3.url,
          "name" -> schema3.name,
          "resourceType" -> SchemaFormats.STRUCTURE_DEFINITION,
          "type" -> schema3.`type`,
          "fhirVersion" -> "4.0.1",
          "kind" -> "logical",
          "baseDefinition" -> "http://hl7.org/fhir/StructureDefinition/Element",
          "derivation" -> "specialization",
          "status" -> "draft",
          "abstract" -> false
        )

        // Extract the elements of the StructureDefinition to a List
        val differential: Map[String, List[Any]] = schemaResourceMap.get("differential") match {
          case Some(value) => value.asInstanceOf[Map[String, List[Any]]]
          case None => Map.empty
        }
        // Validate if fieldDefinitions length of the SchemaDefinition and element length of the StructureDefinition are the same
        differential.get("element") match {
          case Some(elementList) => elementList should have length 3
          case None => fail("Field definitions are missing in the structure definition of the schema.")
        }
      }

      // get a schema with invalid id by a url including format parameter
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/123123?format=${SchemaFormats.STRUCTURE_DEFINITION}") ~> route ~> check {
        status shouldEqual StatusCodes.NotFound
      }
    }

    "import schema as structure definition" in {
      // blood pressure schema information for the test
      val bloodPressureSchemaUrl: String = "https://aiccelerate.eu/fhir/StructureDefinition/Ext-plt1-hsjd-blood-pressure"
      val bloodPressureSchemaName: String = "blood-pressure-schema"

      // read the StructureDefinition of the schema from file
      val schemaResource: Some[Resource] = Some(FileOperations.readJsonContentAsObject[Resource](FileOperations.getFileIfExists(getClass.getResource("/test-schemas/blood-pressure-schema.json").getPath)))

      // create a schema by using StructureDefinition
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}?format=${SchemaFormats.STRUCTURE_DEFINITION}", HttpEntity(ContentTypes.`application/json`, writePretty(schemaResource))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
      }

      // validate if the schema is imported correctly
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}?url=$bloodPressureSchemaUrl") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual bloodPressureSchemaUrl
        schema.name shouldEqual bloodPressureSchemaName
        val fieldDefinitions = schema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 6
        fieldDefinitions.head.path shouldEqual "Ext-plt1-hsjd-blood-pressure.pid"
        fieldDefinitions.last.path shouldEqual "Ext-plt1-hsjd-blood-pressure.diastolic"
      }
    }

    "import a non-existent StructureDefinition from a FHIR server" in {
      // settings to import a StructureDefinition named 'NonExistentStructureDefinition' from the onFHIR
      val schemaID = "NonExistentStructureDefinition"
      val importSchemaSettings = ImportSchemaSettings(onFhirClient.getBaseUrl(), schemaID, None)
      // import the StructureDefinition from onFHIR
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_IMPORT}", HttpEntity(ContentTypes.`application/json`, writePretty(importSchemaSettings))) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[String]
        response should include(s"Detail: Structure Definition with id '$schemaID' does not exist.")
      }
    }

    "import StructureDefinition from a FHIR server" in {
      // settings to import a StructureDefinition named 'CustomPatient' from the onFHIR
      val schemaID = "CustomPatient"
      val importSchemaSettings = ImportSchemaSettings(onFhirClient.getBaseUrl(), schemaID, None)
      // import the StructureDefinition from onFHIR
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_IMPORT}", HttpEntity(ContentTypes.`application/json`, writePretty(importSchemaSettings))) ~> route ~> check {
        status shouldEqual StatusCodes.OK
      }
      // url of the generated schema
      val schemaURL = "http://example.org/fhir/StructureDefinition/CustomPatient"
      // validate if the schema is imported correctly
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}?url=$schemaURL") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual schemaURL
        schema.id shouldEqual schemaID
        val fieldDefinitions = schema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 23
        val birthDateField = fieldDefinitions.head
        birthDateField.path shouldEqual "Patient.birthDate"
        birthDateField.minCardinality shouldEqual 1
        val extension = fieldDefinitions.lift(1).get
        extension.path shouldEqual "Patient.extension"
        extension.elements.get.size shouldEqual 2
        val extensionLastElement = extension.elements.get.last
        extensionLastElement.path shouldEqual "Patient.extension.nationality"
        extensionLastElement.sliceName.get shouldEqual "nationality"
        extensionLastElement.elements.get.size shouldEqual 3
        extensionLastElement.elements.get.head.path shouldEqual "Patient.extension.nationality.url"
      }
    }

    "import StructureDefinitions from a ZIP of FHIR Profiles" in {
      // create the FormData including the ZIP file
      val zipFilePath = Paths.get(getClass.getResource("/fhir-resources/custom-profiles.zip").toURI)
      val zipFileBytes = Files.readAllBytes(zipFilePath)
      val formData = Multipart.FormData(
        Multipart.FormData.BodyPart.Strict(
          "file",
          HttpEntity(ContentType(MediaTypes.`application/zip`), zipFileBytes),
          Map("filename" -> "custom-profiles.zip")
        )
      )
      // import the StructureDefinitions from the ZIP
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_IMPORT_ZIP}", formData) ~> route ~> check {
        status shouldEqual StatusCodes.OK
      }
      // url of the generated Condition schema
      val conditionSchemaURL = "http://example.org/fhir/StructureDefinition/CustomCondition"
      // validate if the schema is imported correctly
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}?url=$conditionSchemaURL") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual conditionSchemaURL
        schema.id shouldEqual "CustomCondition"
        val fieldDefinitions = schema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 25
        val birthDateField = fieldDefinitions.head
        birthDateField.path shouldEqual "Condition.onsetDateTime"
        birthDateField.minCardinality shouldEqual 1
        val extension = fieldDefinitions.lift(1).get
        extension.path shouldEqual "Condition.extension"
        extension.elements.get.size shouldEqual 2
        val extensionLastElement = extension.elements.get.last
        extensionLastElement.path shouldEqual "Condition.extension.severity"
        extensionLastElement.sliceName.get shouldEqual "severity"
        extensionLastElement.elements.get.size shouldEqual 3
        extensionLastElement.elements.get.head.path shouldEqual "Condition.extension.severity.url"
      }
      // url of the generated Observation schema
      val observationSchemaURL = "http://example.org/fhir/StructureDefinition/CustomObservation"
      // validate if the schema is imported correctly
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}?url=$observationSchemaURL") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        // validate the retrieved schema
        val schema: SchemaDefinition = JsonMethods.parse(responseAs[String]).extract[SchemaDefinition]
        schema.url shouldEqual observationSchemaURL
        schema.id shouldEqual "CustomObservation"
        val fieldDefinitions = schema.fieldDefinitions.get
        fieldDefinitions.size shouldEqual 32
        val birthDateField = fieldDefinitions.head
        birthDateField.path shouldEqual "Observation.valueQuantity"
        birthDateField.minCardinality shouldEqual 1
        val extension = fieldDefinitions.lift(1).get
        extension.path shouldEqual "Observation.extension"
        extension.elements.get.size shouldEqual 2
        val extensionLastElement = extension.elements.get.last
        extensionLastElement.path shouldEqual "Observation.extension.observationMethod"
        extensionLastElement.sliceName.get shouldEqual "observationMethod"
        extensionLastElement.elements.get.size shouldEqual 3
        extensionLastElement.elements.get.head.path shouldEqual "Observation.extension.observationMethod.url"
      }
    }

    "return an error when importing a FHIR profiles ZIP with missing referenced schemas" in {
      // create the FormData including the ZIP file
      val zipFilePath = Paths.get(getClass.getResource("/fhir-resources/fhir-profiles-missing-referenced-schemas.zip").toURI)
      val zipFileBytes = Files.readAllBytes(zipFilePath)
      val formData = Multipart.FormData(
        Multipart.FormData.BodyPart.Strict(
          "file",
          HttpEntity(ContentType(MediaTypes.`application/zip`), zipFileBytes),
          Map("filename" -> "fhir-profiles-missing-referenced-schemas.zip")
        )
      )
      // import the StructureDefinitions from the ZIP
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}/${SchemaDefinitionEndpoint.SEGMENT_IMPORT_ZIP}", formData) ~> route ~> check {
        status shouldEqual StatusCodes.BadRequest
        val response = responseAs[String]
        response should include("Detail: The schema with URL 'https://aiccelerate.eu/fhir/StructureDefinition/AIC-Condition' references a non-existent schema: 'https://aiccelerate.eu/fhir/StructureDefinition/AIC-Patient'. Ensure all referenced schemas exist.")
      }
    }

    /**
     * Test case to verify retrieval of simplified element definitions for a given schema.
     *
     * This test performs the following steps:
     * 1. Creates a schema with two elements and posts it to the Schema Definition endpoint.
     * 2. Sends a GET request to the Schema Definition endpoint with the schema URL and SimpleStructureDefinition format
     * and expects a sequence of SimpleStructureDefinition objects to be returned in the response.
     */
    "retrieve simplified element definitions of a schema" in {
      // create a schema with two elements
      val schemaUrl: String = "https://example.com/fhir/StructureDefinition/schema"
      val schema: SchemaDefinition = SchemaDefinition(url = schemaUrl, version = "1.4.2", `type` = "Ty", name = "name", description = Some("description"), rootDefinition = None, fieldDefinitions = Some(Seq(
        SimpleStructureDefinition(id = "element-with-definition",
          path = "Ty.element-with-definition", dataTypes = Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.CANONICAL, profiles = Some(Seq("http://hl7.org/fhir/StructureDefinition/canonical"))))), isPrimitive = true,
          isChoiceRoot = false, isArray = false, minCardinality = 0, maxCardinality = None,
          boundToValueSet = None, isValueSetBindingRequired = None, referencableProfiles = None, constraintDefinitions = None, sliceDefinition = None,
          sliceName = None, fixedValue = None, patternValue = None, referringTo = None, short = Some("element-with-definition"), definition = Some("element definition"), comment = None, elements = None),
        SimpleStructureDefinition(id = "element-with-no-definition",
          path = "Ty.element-with-no-definition", dataTypes = Some(Seq(DataTypeWithProfiles(dataType = FHIR_DATA_TYPES.CANONICAL, profiles = Some(Seq("http://hl7.org/fhir/StructureDefinition/canonical"))))), isPrimitive = true,
          isChoiceRoot = false, isArray = false, minCardinality = 0, maxCardinality = None,
          boundToValueSet = None, isValueSetBindingRequired = None, referencableProfiles = None, constraintDefinitions = None, sliceDefinition = None,
          sliceName = None, fixedValue = None, patternValue = None, referringTo = None, short = Some("element-with-no-definition"), definition = None, comment = None, elements = None)
      )))
      Post(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}", HttpEntity(ContentTypes.`application/json`, writePretty(schema))) ~> route ~> check {
        status shouldEqual StatusCodes.Created
      }
      // retrieve the simplified element definitions of this schema
      Get(s"/${webServerConfig.baseUri}/${ProjectEndpoint.SEGMENT_PROJECTS}/$projectId/${SchemaDefinitionEndpoint.SEGMENT_SCHEMAS}?$QUERY_PARAM_TYPE=${SchemaFormats.SIMPLE_STRUCTURE_DEFINITION}&$QUERY_PARAM_URL=$schemaUrl") ~> route ~> check {
        status shouldEqual StatusCodes.OK
        JsonMethods.parse(responseAs[String]).extract[Seq[SimpleStructureDefinition]].length shouldEqual 2
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
    // create a test patient profile on the onFHIR server.
    // this profile will be used later to generate a schema.
    val testPatientProfile: Resource = JsonMethods.parse(Source.fromInputStream(getClass.getResourceAsStream("/fhir-resources/custom-patient-profile.json")).mkString).extract[Resource]
    onFhirClient.batch()
      .entry(_.update(testPatientProfile))
      .returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
      res.httpStatus shouldBe StatusCodes.OK
    }
  }

  /**
   * Drop tables after schema inferring.
   * */
  override def afterAll(): Unit = {
    val sql = readFileContent("/sql/sql-source-drop.sql")
    runSQL(sql)
    super.afterAll()
    // delete test resources on onFHIR
    onFhirClient.batch()
      .entry(_.delete("StructureDefinition"))
      .returnMinimal().asInstanceOf[FhirBatchTransactionRequestBuilder].execute() map { res =>
      res.httpStatus shouldBe StatusCodes.OK
    }
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
