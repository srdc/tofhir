package io.onfhir.tofhir.engine

import akka.actor.ActorSystem

object Execution {
  implicit lazy val actorSystem: ActorSystem = ActorSystem("FhirRepositoryWriter")
}
