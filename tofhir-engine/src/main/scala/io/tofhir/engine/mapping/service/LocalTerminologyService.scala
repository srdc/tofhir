package io.tofhir.engine.mapping.service

import com.fasterxml.jackson.dataformat.csv.CsvReadException
import io.onfhir.api.Resource
import io.onfhir.api.service.IFhirTerminologyService
import io.onfhir.api.util.{FHIRUtil, IOUtil}
import io.tofhir.engine.mapping.service.LocalTerminologyService.{CodeSystemFileColumns, ConceptMapFileColumns, equivalenceCodes}
import io.tofhir.engine.model.exception.FhirMappingException
import io.tofhir.engine.model.{ConceptMapFile, LocalFhirTerminologyServiceSettings}
import io.tofhir.engine.util.{CsvUtil, FileUtils}
import org.json4s.{JArray, JBool, JObject, JString}

import java.io.File
import java.util.concurrent.TimeUnit
import io.tofhir.engine.Execution.actorSystem.dispatcher
import scala.concurrent.Future
import scala.concurrent.duration.Duration


/**
 * Simulation of a terminology service (for translation and lookup) from given concept map and code system data in a folder in csv file format
 * @param settings  Settings for the local terminology service
 */
class LocalTerminologyService(settings:LocalFhirTerminologyServiceSettings) extends IFhirTerminologyService with Serializable{
  //All csv files given in the configured folder
  val relatedFiles:Map[String, File] =
    IOUtil
      .getFilesFromFolder(new File(FileUtils.getPath(settings.folderPath).toUri), recursively = true, ignoreHidden = true, withExtension = Some(FileUtils.FileExtensions.CSV.toString))
      .map(f => f.getName -> f)
      .toMap

  /**
   * A simple in-memory concept map; source (system,code) -> target (system, code, display, equivalence)
   */
  val conceptMaps:Seq[(ConceptMapFile, Map[(String, String), Seq[(String, String, Option[String], String)]])] =
    settings
      .conceptMapFiles
      .map(cmf =>
        cmf ->
          parseConceptMapGivenInCsv(
            relatedFiles.get(cmf.name) match {
              case None => throw FhirMappingException(s"Missing toFHIR ConceptMap CSV file '${cmf.name}' within configured folder '${settings.folderPath}'!")
              case Some(f)=> f.getAbsolutePath
            }
      ))

  /**
   * A simple in-memory code system for lookup operations system -> (code -> (display, designations))
   */
  val codeSystems:Map[String, Map[String, (String, Map[String, String])]] =
    settings
      .codeSystemFiles
      .map(cs => cs.codeSystem -> parseCodeSystemGivenInCsv(
        relatedFiles.get(cs.name) match {
          case None => throw FhirMappingException(s"Missing tofhir CodeSystem CSV file '${cs.name}' within configured folder '${settings.folderPath}'!")
          case Some(f)=> f.getAbsolutePath
        }
      ))
      .toMap

  override def getTimeout: Duration = Duration.apply(1, TimeUnit.MINUTES)

  /**
   * Translate the given code + system based on given conceptMapUrl
   * @param code                A FHIR code value
   * @param system              FHIR Code system url
   * @param conceptMapUrl       A canonical URL for a concept map
   * @param version             The version of the system, if one was provided in the source data
   * @param conceptMapVersion   The identifier that is used to identify a specific version of the concept map to be used for the translation.
   * @param reverse             If this is true, then the operation should return all the codes that might be mapped to this code.
   * @return                    Resulting Parameters resource
   */
  override def translate(code: String, system: String, conceptMapUrl: String, version: Option[String], conceptMapVersion: Option[String], reverse: Boolean): Future[JObject] = {
    Future.apply {
      if (version.isDefined || conceptMapVersion.isDefined || reverse)
        throw FhirMappingException("Only simple translate statements are supported in mappings with a local terminology service!, 'version', 'conceptMapVersion', 'reverse' parameters are not supported!")

      conceptMaps
        .find(_._1.conceptMapUrl == conceptMapUrl)
        .map(_._2)
        .map(conceptMap =>
          conceptMap
            .get(system -> code)
            .map(matchings => constructParametersResourceForTranslate(matchings))
            .getOrElse(constructParametersResourceForNoTranslation(s"Source system and code ($system, $code) not found withing the concept map $conceptMapUrl!"))
        ).getOrElse(constructParametersResourceForNoTranslation(s"Concept map with url $conceptMapUrl is not found!"))
    }
  }

  /**
   * Translate the given code + system based on given conceptMapUrl
   * @param code                A FHIR code value
   * @param system              FHIR Code system url
   * @param conceptMapUrl       A canonical URL for a concept map
   * @return                    Resulting Parameters resource
   */
  override def translate(code: String, system: String, conceptMapUrl: String): Future[JObject] = translate(code, system, conceptMapUrl, None, None, reverse=false)

  /**
   * Translate the given Coding or codeable concept based on given conceptMapUrl
   * @param codingOrCodeableConcept   FHIR Coding or CodeableConcept
   * @param conceptMapUrl             A canonical URL for a concept map
   * @param conceptMapVersion         The identifier that is used to identify a specific version of the concept map to be used for the translation.
   * @param reverse                   If this is true, then the operation should return all the codes that might be mapped to this code.
   * @return                          Resulting Parameters resource if successful
   */
  override def translate(codingOrCodeableConcept: JObject, conceptMapUrl: String, conceptMapVersion: Option[String], reverse: Boolean): Future[JObject] = {
    Future.apply {
      if (conceptMapVersion.isDefined | reverse)
        throw FhirMappingException("Only simple translate statements are supported in mappings with a local terminology service!, The 'conceptMapVersion' or 'reverse' parameters are not supported!")

      conceptMaps
        .find(_._1.conceptMapUrl == conceptMapUrl)
        .map(_._2)
        .map(conceptMap =>
          getSystemCodesFromCodeableConceptOrCoding(codingOrCodeableConcept) match {
            case Nil => constructParametersResourceForNoTranslation(s"Given FHIR Coding or CodeableConcept element does not include system or code fields!")
            case Seq(system -> code) =>
              conceptMap
                .get(system -> code)
                .map(matches => constructParametersResourceForTranslate(matches))
                .getOrElse(constructParametersResourceForNoTranslation(s"Source system and code ($system, $code) not found withing the concept map $conceptMapUrl!"))
            case multipleCodes =>
              multipleCodes
                .flatMap(sc => conceptMap.get(sc))
                .flatten match {
                    case Nil => constructParametersResourceForNoTranslation(s"Code and system pairs given in CodeableConcept are not found within the concept map $conceptMapUrl!")
                    case matches => constructParametersResourceForTranslate(matches)
                }
          }
        )
        .getOrElse(constructParametersResourceForNoTranslation(s"Concept map with url $conceptMapUrl is not found!"))
    }
  }

  /**
   * Translate the given Coding or codeable concept based on given conceptMapUrl
   * @param codingOrCodeableConcept   FHIR Coding or CodeableConcept
   * @param conceptMapUrl             A canonical URL for a concept map
   * @return                          Resulting Parameters resource
   */
  override def translate(codingOrCodeableConcept: JObject, conceptMapUrl: String): Future[JObject] = translate(codingOrCodeableConcept, conceptMapUrl, None, reverse =false)

  /**
   * Translate the given code + system based on given source and target value sets
   * @param code        A FHIR code value
   * @param system      FHIR Code system url
   * @param source      Identifies the value set used when the concept (system/code pair) was chosen. May be a canonical url for the valueset, or an absolute or relative location.
   * @param target      Identifies the value set in which a translation is sought.
   * @param version     The version of the system, if one was provided in the source data
   * @param reverse     If this is true, then the operation should return all the codes that might be mapped to this code.
   * @return            Resulting Parameters resource
   */
  override def translate(code: String, system: String, source: Option[String], target: Option[String], version: Option[String], reverse: Boolean): Future[JObject] = {
    Future.apply {
      if (version.isDefined || reverse)
        throw FhirMappingException("Only simple translate statements are supported in mappings with a local terminology service!. The 'version' or 'reverse' parameters are not supported!")

      if (source.isEmpty && target.isEmpty)
        throw FhirMappingException("Only simple translate statements are supported in mappings with a local terminology service!, The 'source' or 'target' parameter should be given!")

      conceptMaps
        .filter(cm => source.forall(_ == cm._1.sourceValueSetUrl) && target.forall(_ == cm._1.targetValueSetUrl))
        .map(_._2) match {
          case Nil => constructParametersResourceForNoTranslation(s"Concept map from source value set ${source.getOrElse("?")} to target value set ${target.getOrElse("?")} is not found!")
          case foundConceptMaps =>
            foundConceptMaps.flatMap(conceptMap => conceptMap.get(system -> code)).flatten match {
              case Nil => constructParametersResourceForNoTranslation(s"Source system and code ($system, $code) not found within any concept map from source value set ${source.getOrElse("?")} to target value set ${target.getOrElse("?")}!")
              case matches => constructParametersResourceForTranslate(matches)
            }
        }
    }
  }
  /**
   * Translate the given code + system based on given source and target value sets
   * @param code        A FHIR code value
   * @param system      FHIR Code system url
   * @param source      Identifies the value set used when the concept (system/code pair) was chosen. May be a canonical url for the valueset, or an absolute or relative location.
   * @param target      Identifies the value set in which a translation is sought.
   * @return            Resulting Parameters resource
   */
  override def translate(code: String, system: String, source: Option[String], target: Option[String]): Future[JObject] = translate(code, system, source, target, None, reverse=false)

  /**
   * Translate the given Coding or codeable concept based on given source and target value sets
   * @param codingOrCodeableConcept    FHIR Coding or CodeableConcept
   * @param source      Identifies the value set used when the concept (system/code pair) was chosen. May be a canonical url for the valueset, or an absolute or relative location.
   * @param target      Identifies the value set in which a translation is sought.
   * @param reverse     If this is true, then the operation should return all the codes that might be mapped to this code.
   * @return            Resulting Parameters resource
   */
  override def translate(codingOrCodeableConcept: JObject, source: Option[String], target: Option[String], reverse: Boolean): Future[JObject] = {
    Future.apply {
      if(reverse)
        throw FhirMappingException("Only simple translate statements are supported in mappings with a local terminology service! The 'reverse' parameter is not supported!")

      conceptMaps
        .filter(cm => source.forall(_ == cm._1.sourceValueSetUrl) && target.forall(_ == cm._1.targetValueSetUrl))
        .map(_._2) match {
        case Nil => constructParametersResourceForNoTranslation(s"Concept map from source value set ${source.getOrElse("?")} to target value set ${target.getOrElse("?")} is not found!")
        case foundConceptMaps =>
          getSystemCodesFromCodeableConceptOrCoding(codingOrCodeableConcept) match {
            case Nil => constructParametersResourceForNoTranslation(s"Given FHIR Coding or CodeableConcept element does not include system or code fields!")
            case Seq(system -> code) =>
              foundConceptMaps.flatMap(_.get(system -> code)).flatten match {
                case Nil => constructParametersResourceForNoTranslation(s"Source system and code ($system, $code) not found withing the concept map from source value set ${source.getOrElse("?")} to target value set ${target.getOrElse("?")}!")
                case matches =>  constructParametersResourceForTranslate(matches)
              }
            case multipleCodes =>
              multipleCodes
                .flatMap(sc => foundConceptMaps.flatMap(_.get(sc)))
                .flatten match {
                    case Nil => constructParametersResourceForNoTranslation(s"Code and system pairs given in CodeableConcept are not found within the concept map from source value set ${source.getOrElse("?")} to target value set ${target.getOrElse("?")}!")
                    case matches => constructParametersResourceForTranslate(matches)
              }
          }
      }
    }
  }

  /**
   * Extract system and code from CodeableConcept or Coding
   * @param codingOrCodeableConcept Json object
   * @return
   */
  private def getSystemCodesFromCodeableConceptOrCoding(codingOrCodeableConcept:JObject):Seq[(String, String)] = {
    codingOrCodeableConcept.obj
      .find(_._1 == "coding").map(_._2) match {
      //If it is a codeable concept
      case Some(JArray(codings)) =>
        //Find out all system,code pairs within codeable concept
        codings
            .filter(_.isInstanceOf[JObject])
            .map(_.asInstanceOf[JObject])
            .map(c => FHIRUtil.extractValueOption[String](c, "system") ->
              FHIRUtil.extractValueOption[String](c, "code")
            ).filter(c => c._1.isDefined && c._2.isDefined)
            .map(c => c._1.get -> c._2.get)

      case None =>
        val sourceCode = FHIRUtil.extractValueOption[String](codingOrCodeableConcept, "system") -> FHIRUtil.extractValueOption[String](codingOrCodeableConcept, "code")
        sourceCode match {
          case (Some(system), Some(code)) => Seq(system -> code)
          case _ => Nil
        }
    }
  }

  /**
   * Translate the given Coding or codeable concept based on given source and target value sets
   * @param codingOrCodeableConcept    FHIR Coding or CodeableConcept
   * @param source                    Identifies the value set used when the concept (system/code pair) was chosen. May be a canonical url for the valueset, or an absolute or relative location.
   * @param target                    Identifies the value set in which a translation is sought.
   * @return                          Resulting Parameters resource if successful
   */
  override def translate(codingOrCodeableConcept: JObject, source: Option[String], target: Option[String]): Future[JObject] = translate(codingOrCodeableConcept, source, target, reverse = false)


  /**
   * Given a code/system, get additional details about the concept, including definition, status, designations, and properties.
   * @param code            A FHIR code value
   * @param system          FHIR Code system url
   * @param version         The version of the system, if one was provided in the source data
   * @param date            The date for which the information should be returned. Normally, this is the current conditions (which is the default value) but under some circumstances, systems need to acccess this information as it would have been in the past
   * @param displayLanguage The requested language for display (see $expand.displayLanguage)
   * @param properties      A property that the client wishes to be returned in the output. If no properties are specified, the server chooses what to return.
   * @return                Resulting Parameters resource
   */
  override def lookup(code: String, system: String, version: Option[String], date: Option[String], displayLanguage: Option[String], properties: Seq[String]): Future[Option[JObject]] = {
    Future.apply {
      if (version.isDefined || date.isDefined || properties.nonEmpty)
        throw FhirMappingException("Only simple lookup operation is supported in mappings with a local terminology service! The 'version', 'date', or 'properties' parameter is not supported!")

      codeSystems
        .get(system)
        .flatMap(_.get(code))
        .map {
          case (display, designations) => constructParametersResourceForLookup(system, display, designations, displayLanguage)
        }
    }
  }

  /**
   * Given a code/system, get additional details about the concept, including definition, status, designations, and properties.
   * @param code     A FHIR code value
   * @param system   FHIR Code system url
   * @return          Resulting Parameters resource
   */
  override def lookup(code: String, system: String): Future[Option[JObject]] = lookup(code, system, None, None, None, Nil)

  /**
   * Given a Coding, get additional details about the concept, including definition, status, designations, and properties.
   * @param coding        FHIR Coding
   * @param date          The date for which the information should be returned. Normally, this is the current conditions (which is the default value) but under some circumstances, systems need to acccess this information as it would have been in the past
   * @param displayLanguage The requested language for display (see $expand.displayLanguage)
   * @param properties    A property that the client wishes to be returned in the output. If no properties are specified, the server chooses what to return.
   * @return               Resulting Parameters resource if co
   */
  override def lookup(coding: JObject, date: Option[String], displayLanguage: Option[String], properties: Seq[String]): Future[Option[JObject]] = {
    Future.apply {
      if (properties.nonEmpty || date.isDefined)
        throw FhirMappingException("Only simple lookup operation is supported in mappings with a local terminology service! The 'properties' or 'date' parameter is not supported!")

      val (system, code) = FHIRUtil.extractValue[String](coding, "system") -> FHIRUtil.extractValue[String](coding, "code")

      codeSystems
        .get(system)
        .flatMap(_.get(code))
        .map {
          case (display, designations) => constructParametersResourceForLookup(system, display, designations, displayLanguage)
        }
    }
  }

  /**
   * Given a Coding, get additional details about the concept, including definition, status, designations, and properties.
   * @param coding   FHIR Coding
   * @return          Resulting Parameters resource if code is found
   */
  override def lookup(coding: JObject): Future[Option[JObject]] = lookup(coding, None, None, Nil)


  /**
   * Construct FHIR Parameters resource (Response for lookup operation) based on matched concept
   * @param system          Name of the system
   * @param display         Default display for the concept
   * @param designations    Designations for the concept
   * @param displayLanguage Requested display language (If no such designation it will return default display)
   * @return
   */
  private def constructParametersResourceForLookup(system:String, display:String, designations:Map[String, String], displayLanguage:Option[String]):JObject = {
    JObject(
      "parameter" -> JArray(
        List(
          JObject("name" -> JString("name"), "valueString" -> JString(system)),
          JObject("name" -> JString("display"), "valueString" -> JString(displayLanguage.flatMap(l => designations.get(l)).getOrElse(display))),
        )
      )
    )
  }

  /**
   * Construct FHIR Parameters resource (Response for translate operation) based on matched concepts
   * @param matchings Matched concepts (system, code, display, equivalance)
   * @return
   */
  private def constructParametersResourceForTranslate(matchings:Seq[(String, String, Option[String], String)]):JObject = {
    JObject(
      "parameter" -> JArray(
        JObject("name" -> JString("result"), "valueBoolean" -> JBool(true)) +:
          matchings.map {
            case (system,code, display, equivalence) =>
              JObject(
                "name" -> JString("match"),
                "part" -> JArray(List(
                  JObject("name" -> JString("relationship"), "valueCode" -> JString(equivalence)),
                  JObject(
                    "name" -> JString("concept"),
                    "valueCoding" -> JObject(
                        List("system" -> JString(system), "code" -> JString(code)) ++ display.map(d => "display" -> JString(d))
                      )
                  )
                ))
              )
          }.toList
      ))
  }

  private def constructParametersResourceForNoTranslation(msg:String):JObject = {
    JObject(
      "parameter" -> JArray(List(
        JObject("name" -> JString("result"), "valueBoolean" -> JBool(false)),
        JObject("name" -> JString("message"), "valueString" -> JString(msg)),
      ))
    )
  }

  /**
   * Parse the concept map given in CSV format
   * @param filePath File path
   * @return  A map of (source system, source code) -> (target system, target code, target display, equivalence)
   */
  private def parseConceptMapGivenInCsv(filePath:String): Map[(String, String), Seq[(String, String, Option[String], String)]] = {
    try {
      CsvUtil
        .readFromCSV(filePath)
        .map(columns =>
          (columns(ConceptMapFileColumns.SOURCE_SYSTEM) -> columns(ConceptMapFileColumns.SOURCE_CODE)) ->
            (
              columns(ConceptMapFileColumns.TARGET_SYSTEM),
              columns(ConceptMapFileColumns.TARGET_CODE),
              columns.get(ConceptMapFileColumns.TARGET_DISPLAY).filterNot(c => c == "" || c == null),
              columns.get(ConceptMapFileColumns.EQUIVALENCE).filter(e =>  equivalenceCodes.contains(e)).getOrElse("equivalent") //If there is no equivalence column, we assume all matching are equivalent
            )
        ).groupBy(_._1).view.mapValues(_.map(_._2))
        .toMap
    } catch {
      case t:CsvReadException =>
        throw FhirMappingException(s"Invalid tofhir concept map CSV file $filePath!",t)
      case t:Throwable =>
        throw FhirMappingException(s"Invalid tofhir concept map CSV file $filePath! Columns ${Set(ConceptMapFileColumns.SOURCE_SYSTEM, ConceptMapFileColumns.SOURCE_CODE, ConceptMapFileColumns.TARGET_SYSTEM, ConceptMapFileColumns.TARGET_CODE).mkString(",")} are mandatory for concept map!",t)
    }
  }

  /**
   * Parse the code system definition given in CSV format
   * @param filePath  File path
   * @return          A map of code -> (display, designations)
   */
  private def parseCodeSystemGivenInCsv(filePath:String):Map[String, (String, Map[String, String])] = {
    try {
      CsvUtil
        .readFromCSV(filePath)
        .map(columns =>
          columns(CodeSystemFileColumns.CODE) ->
            (
              columns(CodeSystemFileColumns.DISPLAY),
              columns
                .filterNot(c => c._1 == CodeSystemFileColumns.CODE || c._1 == CodeSystemFileColumns.DISPLAY)
                .filterNot(c => c._2 == null || c._2 == "")
            )
        )
        .toMap
    } catch {
      case t:Throwable =>
        throw FhirMappingException(s"Invalid tofhir CodeSystem CSV file $filePath! Columns ${Set(CodeSystemFileColumns.CODE, CodeSystemFileColumns.DISPLAY).mkString(",")} are mandatory for code system definitions!",t)
    }
  }

  /**
   * These methods are not implemented for the LocalTerminologyService and will not be implemented in the future.
   */
  override def expandWithId(id: String, filter: Option[String], offset: Option[Long], count: Option[Long]): Future[JObject] = ???
  override def expand(url: String, version: Option[String], filter: Option[String], offset: Option[Long], count: Option[Long]): Future[JObject] = ???
  override def expandWithValueSet(valueSet: Resource, offset: Option[Long], count: Option[Long]): Future[JObject] = ???
  override def validateCode(url: String, valueSetVersion: Option[String], code: String, system: Option[String], systemVersion: Option[String], display: Option[String]): Future[JObject] = ???
}

object LocalTerminologyService {
  /**
   * Columns of CSV file to represent FHIR Concept Map in a practical way
   */
  object ConceptMapFileColumns {
    final val SOURCE_CODE = "source_code"
    final val SOURCE_SYSTEM = "source_system"
    final val SOURCE_DISPLAY = "source_display"
    final val TARGET_CODE = "target_code"
    final val TARGET_SYSTEM = "target_system"
    final val TARGET_DISPLAY = "target_display"
    final val EQUIVALENCE = "equivalence"
  }

  /**
   * Columns of CSV file to represent FHIR CodeableConcept in a practical way
   */
  object CodeSystemFileColumns {
    final val CODE = "code"
    final val DISPLAY = "display"
  }

  /**
   * FHIR ConceptMap equivalence code
   */
  val equivalenceCodes = Set("relatedto", "equivalent", "equal", "wider", "subsumes", "narrower", "specializes", "inexact", "unmatched", "disjoint")

}
