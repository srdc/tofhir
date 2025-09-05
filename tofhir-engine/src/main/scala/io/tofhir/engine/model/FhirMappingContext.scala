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
 * @param conversionFunctions Optionally given unit conversion data
 *
 *                            If the parsed CSV uses "source_unit, target_unit, conversion_function" columns on top of the essential column data, the model will also hold those as Unit Conversion specifications.
 *
 *                            For example, in a composite mapping context like the following, the conversion specific data looks as given below it:
 *
 *                            -----------------------------
 *                            source_code,source_unit,target_code,target_unit,conversion_function
 *                            "1988-5","mg/L","1988-5","mg/L","$this"
 *                            "59260-0","mmol/L","718-7","g/L","$this * 16.114"
 *                            -----------------------------
 *                            Map(
 *                              ("1988-5","mg/L") -> ("mg/L", "$this"),
 *                              ("59260-0", "mmol/L")-> ("g/L",  "$this * 16.114")
 *                            )
 *
 *
 */
case class ConceptMapContext(concepts: Map[String, Seq[Map[String, String]]], conversionFunctions: Map[(String, String), (String, String)] = Map.empty) extends FhirMappingContext {
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
