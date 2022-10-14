package io.tofhir.engine.mapping

import akka.actor.ActorSystem

object Execution {
  implicit lazy val actorSystem: ActorSystem = ActorSystem("toFHIR")
}
