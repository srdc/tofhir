package io.onfhir.tofhir

import org.scalatest._
import org.scalatest.flatspec._
import org.scalatest.matchers._

abstract class ToFhirTestSpec extends AnyFlatSpec with should.Matchers with
  OptionValues with Inside with Inspectors
