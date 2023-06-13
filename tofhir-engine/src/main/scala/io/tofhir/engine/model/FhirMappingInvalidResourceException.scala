package io.tofhir.engine.model

/**
 * When the mapping results are written to a Fhir server, we might get errors because some results are not valid
 * FHIR resources or they do not conform to the indicated profiles.
 * This is the exception to be thrown when we end up with invalid resources in the Fhir Server response.
 *
 * @param reason                the reason of exception
 * @param numOfInvalidResources the number of invalid resources in the mapping
 * @param cause                 the throwable that caused this exception to get thrown.
 * */
final case class FhirMappingInvalidResourceException(private val reason: String, private val numOfInvalidResources: Int, private val cause: Throwable = None.orNull)
  extends Exception(reason: String, cause: Throwable) {
  /**
   * Returns the number of invalid resources.
   *
   * @return the number of invalid resources
   * */
  def getNumOfInvalidResources: Int = numOfInvalidResources
}