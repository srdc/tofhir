package io.tofhir.engine.util

object FhirVersionUtil {

  /**
   * Get the major version of FHIR from
   * @param fhirVersion formal version of the FHIR standard e.g. 4.0.1, 5.0.0
   * @return
   */
  def getMajorFhirVersion(fhirVersion: String): String = {
    fhirVersion match {
      case version if version.startsWith("4") => MajorFhirVersion.R4
      case version if version.startsWith("5") => MajorFhirVersion.R5
      case _ => throw new IllegalArgumentException(s"Unsupported FHIR version: $fhirVersion")
    }
  }

}
 /* enumeration for major FHIR versions */
object MajorFhirVersion {
  val R4 = "R4"
  val R5 = "R5"
}
