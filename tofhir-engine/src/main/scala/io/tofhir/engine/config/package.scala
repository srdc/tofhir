package io.tofhir.engine

package object config {

  object ErrorHandlingType extends Enumeration {
    type ErrorHandlingType = Value
    final val CONTINUE = Value("continue")
    final val HALT = Value("halt")
  }

}