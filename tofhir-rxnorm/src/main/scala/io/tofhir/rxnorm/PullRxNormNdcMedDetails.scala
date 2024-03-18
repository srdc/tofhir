package io.tofhir.rxnorm

import java.io.{FileReader, FileWriter}
import com.opencsv.{CSVReaderBuilder, CSVWriter}
import io.onfhir.api.util.FHIRUtil
import org.json4s.JsonAST.{JArray, JDouble, JObject, JString}
import org.json4s._
import io.onfhir.util.JsonFormatter._

object PullRxNormNdcMedDetails extends App {
  // RxNorm Api client for calls
  val rxNormApiClient = RxNormApiClient("https://rxnav.nlm.nih.gov", 10)

  // Read ncd codes from prescription table
  val csvFilePath = "mimic-iv-data/hosp/prescriptions.csv"

  // Open the CSV file reader
  val fileReader = new FileReader(csvFilePath)
  val csvReader = new CSVReaderBuilder(fileReader).withSkipLines(1).build() // Skip header line

  // For unique ndc codes
  var ndcSet = scala.collection.mutable.Set[String]()

  try {
    // Iterate over each row in the CSV file
    var record = csvReader.readNext()

    while (record != null) {
      // Extract the "ndc" field
      val ndcField = record(11)

      // Add it into the set
      ndcSet += ndcField

      // Read the next record
      record = csvReader.readNext()
    }
    // Convert it into immutable set
    val ndcSetImmutable = ndcSet.toSet
    println(s"number of ndc codes: ${ndcSetImmutable.size}")

    // Create FileWriter object
    val fileWriter = new FileWriter("ndcToMedDetails.csv")

    // Create CSVWriter object
    val csvWriter = new CSVWriter(fileWriter)

    try{
      // Set headers for the csv file
      val header = Array("ndc", "doseFormRxcui", "doseFormName", "activeIngredientRxcui", "activeIngredientName",
        "numeratorUnit", "numeratorValue", "denominatorUnit", "denominatorValue")
      csvWriter.writeNext(header)

      ndcSetImmutable.foreach(ndc => {
        if(ndc.nonEmpty){
          // Get conceptIds from RxNorm Api
          try{
            val conceptIds = rxNormApiClient.findRxConceptIdByNdc(ndc);

            conceptIds
              .iterator
              .filter(_.nonEmpty)
              .flatMap(rxcui =>
                rxNormApiClient.getRxcuiHistoryStatus(rxcui)
              )
              .map(response => {
                // Get the ingredient details
                val ingredients =
                  FHIRUtil
                    .extractValueOptionByPath[Seq[JObject]](response, "rxcuiStatusHistory.definitionalFeatures.ingredientAndStrength")
                    .getOrElse(Nil)
                val ingredientObjs =
                  ingredients
                    .map(i => {
                      val requiredFields =
                        Seq("activeIngredientRxcui", "activeIngredientName", "numeratorValue", "numeratorUnit", "denominatorValue", "denominatorUnit")
                          .map(f =>
                            FHIRUtil.extractValueOption[String](i, f)
                              .filter(_ != "")  // Should filter empty string values
                              .map(v =>
                                if(f == "numeratorValue" || f == "denominatorValue")
                                  f -> JDouble(v.toDouble)
                                else
                                  f ->   JString(v)
                              )
                          )
                      if (requiredFields.forall(_.isDefined))
                        Some(JObject(requiredFields.map(_.get).toList))
                      else
                        None
                    })

                val doseFormObj =
                  FHIRUtil
                    .extractValueOptionByPath[Seq[JObject]](response, "rxcuiStatusHistory.definitionalFeatures.doseFormConcept")
                    .getOrElse(Nil)
                    .headOption
                if(ingredientObjs.nonEmpty && ingredientObjs.forall(_.isDefined)){
                  Some(
                    JObject(
                      List(
                        "ingredientAndStrength" -> JArray(ingredientObjs.map(_.get).toList)
                      ) ++
                        doseFormObj
                          .map(d => "doseFormConcept" -> d)
                          .toSeq
                    )
                  )
                } else
                  None
              })
              .find(_.isDefined).foreach(r => {

              val doseFormRxcui = (r.get \ "doseFormConcept" \ "doseFormRxcui").extract[String]
              val doseFormName = (r.get \ "doseFormConcept" \ "doseFormName").extract[String]
              val activeIngredientRxcui = ((r.get \ "ingredientAndStrength")(0) \ "activeIngredientRxcui").extract[String]
              val activeIngredientName = ((r.get \ "ingredientAndStrength")(0) \ "activeIngredientName").extract[String]
              val numeratorUnit = ((r.get \ "ingredientAndStrength")(0) \ "numeratorUnit").extract[String]
              val numeratorValue = ((r.get \ "ingredientAndStrength")(0) \ "numeratorValue").extract[String]
              val denominatorUnit = ((r.get \ "ingredientAndStrength")(0) \ "denominatorUnit").extract[String]
              val denominatorValue = ((r.get \ "ingredientAndStrength")(0) \ "denominatorValue").extract[String]
              csvWriter.writeNext(Array(ndc, doseFormRxcui, doseFormName, activeIngredientRxcui, activeIngredientName,
                numeratorUnit, numeratorValue, denominatorUnit, denominatorValue))

            }
            )
          }catch {
            case e: Throwable => println(e)
          }
        }
      })
    } finally {
      // Close the CSV write
      fileWriter.close()
      csvWriter.close()
    }

  } finally {
    // Close the CSV reader
    fileReader.close()
    csvReader.close()
  }
}
