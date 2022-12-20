package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.onfhir.config.{BaseFhirConfig, FSConfigReader, IFhirConfigReader}
import io.onfhir.r4.config.FhirR4Configurator
import io.tofhir.server.model.SimpleStructureDefinition

class SchemaDefinitionService(schemaRepositoryFolderPath: String) extends LazyLogging {

  private val fhirConfigReader: IFhirConfigReader = new FSConfigReader(profilesPath = Some(schemaRepositoryFolderPath))

  private val baseFhirConfig: BaseFhirConfig = new FhirR4Configurator().initializePlatform(fhirConfigReader)
  private val simpleStructureDefinitionService = new SimpleStructureDefinitionService(baseFhirConfig)

  def getAllSchemaDefinitions: Seq[SimpleStructureDefinition] = {
    val aicProfiles = baseFhirConfig.profileRestrictions.filter(a=>a._1.startsWith("https://aiccelerate.eu/fhir/StructureDefinition"))
    val a = baseFhirConfig.findProfile("https://aiccelerate.eu/fhir/StructureDefinition/Ext-plt2-condition")
    Seq.empty[SimpleStructureDefinition]
  }

}
