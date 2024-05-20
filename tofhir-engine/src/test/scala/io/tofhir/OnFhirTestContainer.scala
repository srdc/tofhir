package io.tofhir

import io.tofhir.engine.Execution.actorSystem
import io.onfhir.client.OnFhirNetworkClient
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.utility.DockerImageName

/**
 * Initializer of the onFHIR test containers used in toFHIR testing.
 */
trait OnFhirTestContainer {
//
//  implicit val actorSystem: ActorSystem = ActorSystem("toFhirEngineTest") // required for OnFhirNetworkClient

  val onFhirClient = initializeOnFhirClient

  /**
   * Deploy an onFhir container for testing purpose and return its client.
   * @param actorSystem
   * @return
   */
  def initializeOnFhirClient: OnFhirNetworkClient = {
    val ONFHIR_TESTCONTAINER_PORT = 8981 // Port number of the onfhir instance to be created for testing.

    @Container
    val container: GenericContainer[Nothing] = new GenericContainer(DockerImageName.parse("srdc/onfhir:r4")).withExposedPorts(ONFHIR_TESTCONTAINER_PORT)

    container.addEnv("DB_EMBEDDED", "true")
    container.addEnv("SERVER_PORT", ONFHIR_TESTCONTAINER_PORT.toString)
    container.addEnv("SERVER_BASE_URI", "fhir")
    container.addEnv("FHIR_ROOT_URL", s"http://${container.getHost}:$ONFHIR_TESTCONTAINER_PORT/fhir")
    container.withReuse(true)
    container.waitingFor(Wait.forHttp("/fhir").forStatusCode(200))
    container.start()

    OnFhirNetworkClient.apply(s"http://${container.getHost}:${container.getFirstMappedPort}/fhir")
  }

}
