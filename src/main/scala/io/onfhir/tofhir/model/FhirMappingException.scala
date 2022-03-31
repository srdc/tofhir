package io.onfhir.tofhir.model


abstract class FhirMappingException(msg:String) extends Exception(msg) {
  val cause:Option[Throwable] = None
}
