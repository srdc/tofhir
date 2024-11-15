package io.tofhir.server.model

import io.tofhir.engine.model.{FhirMapping, FhirMappingJob}
import org.json4s.{JArray, JObject, JString}

import java.util.UUID

import io.onfhir.definitions.common.model.SchemaDefinition

/**
 * Definition of a project which holds relevant schemas, mapping, mapping-jobs, concept maps etc.
 *
 * @param id                 Unique identifier for the project
 * @param name               Project name
 * @param description        Description of the project
 * @param schemaUrlPrefix    Prefix (beginning) of the URLs to be used while creating schema definitions within this project.
 * @param mappingUrlPrefix   Prefix (beginning) of the URLs to be used while creating mapping definitions within this project.
 * @param schemas            Schemas defined in this project
 * @param mappingContexts    Identifiers of the mapping contexts defined in this project
 * @param mappingJobs        Mapping jobs defined in this project
 */
case class Project(id: String = UUID.randomUUID().toString,
                   name: String,
                   description: Option[String] = None,
                   schemaUrlPrefix: Option[String] = None,
                   mappingUrlPrefix: Option[String] = None,
                   schemas: Seq[SchemaDefinition] = Seq.empty,
                   mappings: Seq[FhirMapping] = Seq.empty,
                   mappingContexts: Seq[String] = Seq.empty,
                   mappingJobs: Seq[FhirMappingJob] = Seq.empty
                  ) {

  /**
   * Extracts the project metadata to be written to the metadata file.
   *
   * @return
   */
  def getMetadata(): JObject = {
    JObject(
      List(
        "id" -> JString(this.id),
        "name" -> JString(this.name),
        "description" -> JString(this.description.getOrElse("")),
        "schemaUrlPrefix" -> JString(this.schemaUrlPrefix.getOrElse("")),
        "mappingUrlPrefix" -> JString(this.mappingUrlPrefix.getOrElse("")),
        "schemas" -> JArray(
          List(
            this.schemas.map(_.getMetadata()): _*
          )
        ),
        "mappings" -> JArray(
          List(
            this.mappings.map(_.getMetadata): _*
          )
        ),
        "mappingContexts" -> JArray(
          List(
            this.mappingContexts.map(cid => JString(cid)): _*
          )
        ),
        "mappingJobs" -> JArray(
          List(
            this.mappingJobs.map(_.getMetadata()): _*
          )
        )
      )
    )
  }
}

/**
 * Editable fields of a project via Patch API
 * */
object ProjectEditableFields {
  val DESCRIPTION = "description"
  val SCHEMA_URL_PREFIX = "schemaUrlPrefix"
  val MAPPING_URL_PREFIX = "mappingUrlPrefix"
}
