package io.onfhir.tofhir.engine

import io.onfhir.api.service.{IFhirIdentityService, IFhirTerminologyService}
import io.onfhir.client.{IdentityServiceClient, TerminologyServiceClient}
import io.onfhir.tofhir.model.{FhirRepositorySinkSettings, IdentityServiceSettings, LocalFhirTerminologyServiceSettings, TerminologyServiceSettings}

import scala.concurrent.ExecutionContext

/**
 * Factory for services that are used within mappings via FHIR Path functions
 */
object IntegratedServiceFactory {
  /**
   * Create corresponding terminology service from the settings
   *
   * @param terminologyServiceSettings Settings for the service
   * @return
   */
  def createTerminologyService(terminologyServiceSettings: TerminologyServiceSettings): IFhirTerminologyService = {
    terminologyServiceSettings match {
      //If this is a FHIR repository settings, it means it is a terminology service
      case terminologyService: FhirRepositorySinkSettings =>
        import Execution.actorSystem
        implicit val ec:ExecutionContext = actorSystem.dispatcher
        new TerminologyServiceClient(terminologyService.createOnFhirClient(actorSystem))
      //If we are having a local terminology service
      case localTerminologySettings: LocalFhirTerminologyServiceSettings =>
        new LocalTerminologyService(localTerminologySettings)
    }
  }

  /**
   * Create corresponding identity service from the settings
   * @param identityServiceSettings Settings for the service
   * @param ec
   * @return
   */
  def createIdentityService(identityServiceSettings: IdentityServiceSettings): IFhirIdentityService = {
    identityServiceSettings match {
      //If identity service will be based on FHIR repo
      case fhirRepositorySinkSettings: FhirRepositorySinkSettings =>
        import Execution.actorSystem
        implicit val ec:ExecutionContext = actorSystem.dispatcher
        new IdentityServiceClient(fhirRepositorySinkSettings.createOnFhirClient(actorSystem))
    }
  }
}
