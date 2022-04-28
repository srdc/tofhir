package io.onfhir.tofhir

package object config {

  object MappingErrorHandling extends Enumeration {
    type MappingErrorHandling = Value
    final val CONTINUE = Value("continue")
    final val HALT = Value("halt")
  }

}
