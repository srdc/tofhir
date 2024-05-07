package io.tofhir

import akka.actor.ActorSystem
import io.onfhir.client.OnFhirNetworkClient
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.execution.RunningJobRegistry
import io.tofhir.engine.mapping._
import io.tofhir.engine.util.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors, OptionValues}
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName

import java.io.FileWriter
import java.net.URI
import scala.io.Source

trait ToFhirTestSpec extends Matchers with OptionValues with Inside with Inspectors {

  val repositoryFolderUri: URI = getClass.getResource(ToFhirConfig.engineConfig.mappingRepositoryFolderPath).toURI
  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(repositoryFolderUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  val schemaRepositoryURI: URI = getClass.getResource(ToFhirConfig.engineConfig.schemaRepositoryFolderPath).toURI
  val schemaRepository = new SchemaFolderLoader(schemaRepositoryURI)

  val sparkSession: SparkSession = ToFhirConfig.sparkSession

  val runningJobRegistry: RunningJobRegistry = new RunningJobRegistry(sparkSession)

  implicit val actorSystem: ActorSystem = ActorSystem("toFhirEngineTest")

  // Instance of OnFhirNetworkClient initialized with onFhir container
  var onFhirClient: OnFhirNetworkClient = initializeOnFhirClient();

  /**
   * Deploy an onFhir container for testing purpose
   * */
  def initializeOnFhirClient(): OnFhirNetworkClient = {
    @Container
    val container: GenericContainer[Nothing] = new GenericContainer(DockerImageName.parse("srdc/onfhir:r4")).withExposedPorts(8081);
    container.addEnv("DB_EMBEDDED", "true");
    container.addEnv("SERVER_PORT", "8081");
    container.addEnv("SERVER_BASE_URI", "fhir");
    container.addEnv("FHIR_ROOT_URL", s"http://${container.getHost}:8081/fhir");
    container.waitingFor(Wait.forHttp("/fhir").forStatusCode(200));
    container.start();
    OnFhirNetworkClient.apply(s"http://${container.getHost}:${container.getFirstMappedPort}/fhir");
  }

  /**
   * Copies the content of a resource file to given location in the context path.
   * @param path The path to the resource file
   * */
  def copyResourceFile(path: String): Unit = {
    // get the content of resource file
    val sourceData = Source.fromResource(path).mkString
    // get the location of resource file according to the context path
    val file = FileUtils.getPath(path).toAbsolutePath.toFile
    // create the parent directories if not exists
    file.getParentFile.mkdirs()
    // create the file
    val fw = new FileWriter(file)
    try fw.write(sourceData) finally fw.close()
  }
}
