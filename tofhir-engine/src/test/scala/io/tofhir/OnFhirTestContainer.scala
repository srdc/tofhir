package io.tofhir

import io.onfhir.client.OnFhirNetworkClient
import io.tofhir.engine.Execution.actorSystem
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.Duration

/**
 * Singleton object responsible for initializing and managing the OnFHIR test container used in toFHIR testing.
 * The container is configured with the necessary environment variables and started with specified settings.
 * Provides a method to obtain an OnFhirNetworkClient instance for interacting with the OnFHIR container.
 */
object OnFhirTestContainer {
  private val ONFHIR_TESTCONTAINER_PORT = 8981 // Port number of the OnFhir instance to be created for testing.
  private val TIMEOUT_SECONDS = 180 // Timeout in seconds for container startup

  private lazy val container: GenericContainer[_] = {
    val container: GenericContainer[Nothing] = new GenericContainer(DockerImageName.parse("srdc/onfhir:r4")).withExposedPorts(ONFHIR_TESTCONTAINER_PORT)
    container.addEnv("DB_EMBEDDED", "true")
    container.addEnv("SERVER_PORT", ONFHIR_TESTCONTAINER_PORT.toString)
    container.addEnv("SERVER_BASE_URI", "fhir")
    container.addEnv("FHIR_ROOT_URL", s"http://${container.getHost}:$ONFHIR_TESTCONTAINER_PORT/fhir")
    container.withReuse(true)
    container.waitingFor(Wait.forHttp("/fhir").forStatusCode(200).withStartupTimeout(Duration.ofSeconds(TIMEOUT_SECONDS)))
    container.start()
    container
  }

  /**
   * Retrieves an OnFhirNetworkClient instance for interacting with the OnFHIR container.
   *
   * @return The singleton onFhirNetworkClient.
   */
  def getOnFhirClient: OnFhirNetworkClient = {
    OnFhirNetworkClient(s"http://localhost:${container.getMappedPort(ONFHIR_TESTCONTAINER_PORT)}/fhir")
  }
}

/**
 * Trait providing access to the OnFHIR test client.
 */
trait OnFhirTestContainer {
  val onFhirClient: OnFhirNetworkClient = OnFhirTestContainer.getOnFhirClient
}