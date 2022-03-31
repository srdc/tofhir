package io.onfhir.tofhir.engine

import io.onfhir.tofhir.model.{FhirMapping, FhirMappingContext}

import scala.concurrent.Future

/**
 * Repository that keeps all mappings and other data in folder
 * @param folderPath  Path to the folder
 */
class FhirMappingFolderRepository(folderPath:String) extends IFhirMappingRepository {


  val fhirMappings:Map[String, FhirMapping] = loadMappings()

  /**
   * Load the mappings from the folder
   * @return
   */
  private def loadMappings():Map[String, FhirMapping] = {
    throw new NotImplementedError()
  }

  /**
   * Return the Fhir mapping definition by given url
   *
   * @param mappingUrl Fhir mapping url
   * @return
   */
  override def getFhirMappingByUrl(mappingUrl: String): Future[FhirMapping] = ???

  override def getFhirMappingContextByUrl:Future[FhirMappingContext] = ???
}
