package io.tofhir.server

package object fhir {
  object FhirAuthMethod extends Enumeration {
    type FhirAuthMethod = Value
    final val BASIC = Value("basic")
    final val BEARER_TOKEN = Value("token")
  }
}
