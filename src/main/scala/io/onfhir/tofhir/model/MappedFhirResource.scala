package io.onfhir.tofhir.model

import org.json4s.JObject

case class MappedFhirResource(rid:String, rtype:String, resource:JObject)
