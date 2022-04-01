package io.onfhir.tofhir.model

import org.json4s.JsonAST.JObject

case class MappedFhirResource(rid:String, rtype:String, resource:JObject) extends Serializable
