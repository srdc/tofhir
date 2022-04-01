package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{FhirMappingContext, FhirMappingContextDefinition}

import scala.concurrent.Future

/**
 * Interface to load a context from definition
 */
trait IMappingContextLoader {
  /**
   * Retrieve the given context data from the definition
   * @param contextDefinition Mapping context definition
   * @return
   */
  def retrieveContext(contextDefinition: FhirMappingContextDefinition):Future[FhirMappingContext]
}

class MappingContextLoader extends IMappingContextLoader {


  def retrieveContext(contextDefinition: FhirMappingContextDefinition):Future[FhirMappingContext] = {
    throw new NotImplementedError()
  }

}
