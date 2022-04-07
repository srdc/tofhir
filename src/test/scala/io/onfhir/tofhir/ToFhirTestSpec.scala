package io.onfhir.tofhir

import org.scalatest._
import org.scalatest.matchers._

abstract class ToFhirTestSpec extends AsyncFlatSpec with should.Matchers with
  OptionValues with Inside with Inspectors
