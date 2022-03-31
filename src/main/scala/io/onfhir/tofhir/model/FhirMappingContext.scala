package io.onfhir.tofhir.model

import org.json4s.JString
import org.json4s.JsonAST.{JField, JObject, JValue}

trait FhirMappingContext {
  def toContextObject:JObject
}

/**
 * Class for loaded concept map context
 * @param concepts  Given concepts with name and columns
 */
case class ConceptMapContext(concepts:Map[String, Map[String, String]]) extends FhirMappingContext {
  override def toContextObject: JObject =
    JObject(concepts.map(c => JField(c._1, JObject(c._2.map(m => JField(m._1, JString(m._2))).toList))).toList)
}

/**
 * Class for loaded unit conversion functions
 * @param conversionFunctions (code of the observation, source unit) -> FHIR Path expression to convert the value to given unit
 *                            e.g. Converting Hemoglogbin to g/dL;   (718-7, g/L) -> $this * 0.1
 */
case class UnitConversionContext(conversionFunctions:Map[(String, String), String]) extends FhirMappingContext {
  override def toContextObject: JObject = JObject()
}

/**
 * Configuration contexts to use in mappings
 * @param configs
 */
case class ConfigurationContext(configs:Map[String, JValue]) extends FhirMappingContext {
  override def toContextObject: JObject = JObject(configs.map(c => JField(c._1, c._2)).toList)
}
