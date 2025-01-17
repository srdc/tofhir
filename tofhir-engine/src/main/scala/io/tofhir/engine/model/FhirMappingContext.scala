package io.tofhir.engine.model

import org.json4s.JsonAST.{JField, JObject, JValue}

trait FhirMappingContext {
  def toContextObject: JObject
}

/**
 * Class for loaded concept map context
 *
 * @param concepts Given concepts with name and columns
 *
 *                 From a CSV who has a header like "source_code,source_system,source_display,unit,profile", and where
 *                 source_code is 9110-8, the concepts Map can be as in the following:
 *
 *                 Map[9110-8 -> Seq [
 *                                    Map[(source_system -> http://loinc.org,Bleeding (cumulative)),
 *                                        (unit -> mL),
 *                                        (profile -> https://aiccelerate.eu/fhir/StructureDefinition/AIC-IntraOperativeObservation)]
 *                 ]]
 *
 *
 */
case class ConceptMapContext(concepts: Map[String, Seq[Map[String, String]]]) extends FhirMappingContext {
  override def toContextObject: JObject = JObject()
}

/**
 * Class for loaded unit conversion functions
 *
 * @param conversionFunctions (code of the observation, source unit) -> FHIR Path expression to convert the value to given unit
 *                            e.g. Converting Hemoglogbin to g/dL;   (718-7, g/L) -> ($this * 0.1, g/dL)
 */
case class UnitConversionContext(conversionFunctions: Map[(String, String), (String, String)]) extends FhirMappingContext {
  override def toContextObject: JObject = JObject()
}

/**
 * Configuration contexts to use in mappings
 *
 * @param configs
 */
case class ConfigurationContext(configs: Map[String, JValue]) extends FhirMappingContext {
  override def toContextObject: JObject = JObject(configs.map(c => JField(c._1, c._2)).toList)
}
