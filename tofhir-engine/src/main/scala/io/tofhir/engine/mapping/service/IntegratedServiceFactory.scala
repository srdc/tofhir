package io.tofhir.engine.mapping.service

import com.typesafe.scalalogging.Logger
import io.onfhir.api.service.{IFhirIdentityService, IFhirTerminologyService}
import io.onfhir.client.{IdentityServiceClient, TerminologyServiceClient}
import io.tofhir.engine.model.{FhirRepositorySinkSettings, IdentityServiceSettings, LocalFhirTerminologyServiceSettings, TerminologyServiceSettings}

import scala.concurrent.ExecutionContext

/**
 * Factory for services that are used within mappings via FHIR Path functions
 */
object IntegratedServiceFactory {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Create corresponding terminology service from the settings
   *
   * @param terminologyServiceSettings Settings for the service
   * @return
   */
  def createTerminologyService(terminologyServiceSettings: TerminologyServiceSettings): IFhirTerminologyService = {
    try {
      terminologyServiceSettings match {
        //If this is a FHIR repository settings, it means it is a terminology service
        case terminologyService: FhirRepositorySinkSettings =>
          import io.tofhir.engine.Execution.actorSystem
          implicit val ec: ExecutionContext = actorSystem.dispatcher
          new TerminologyServiceClient(terminologyService.createOnFhirClient(actorSystem))
        //If we are having a local terminology service
        case localTerminologySettings: LocalFhirTerminologyServiceSettings =>
          new LocalTerminologyService(localTerminologySettings)
      }
    } catch {
      case t: Throwable =>
        logger.error("Failed to create terminology service", t)
        throw t
    }
  }

  /**
   * Create corresponding identity service from the settings
   *
   * @param identityServiceSettings Settings for the service
   * @param ec
   * @return
   */
  def createIdentityService(identityServiceSettings: IdentityServiceSettings): IFhirIdentityService = {
    identityServiceSettings match {
      //If identity service will be based on FHIR repo
      case fhirRepositorySinkSettings: FhirRepositorySinkSettings =>
        import io.tofhir.engine.Execution.actorSystem
        implicit val ec:ExecutionContext = actorSystem.dispatcher
        new IdentityServiceClient(fhirRepositorySinkSettings.createOnFhirClient(actorSystem))
    }
  }
}
