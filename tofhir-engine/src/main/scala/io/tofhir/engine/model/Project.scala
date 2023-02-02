package io.tofhir.engine.model

import java.util.UUID

/**
 * Definition of a project which holds relevant schemas, mapping, mapping-jobs, concept maps etc.
 *
 * @param id          Unique identifier for the project
 * @param name        Project name
 * @param description Description of the project
 */
case class Project(id: String = UUID.randomUUID().toString,
                   name: String,
                   description: Option[String] = None
                  ) {
  /**
   * Validates the fields of a project.
   *
   * @throws IllegalArgumentException when the project id is not a valid UUID
   * */
  def validate(): Unit = {
    // throws IllegalArgumentException if the id is not a valid UUID
    UUID.fromString(id)
  }
}

/**
 * Editable fields of a project via Patch API
 * */
object ProjectEditableFields {
  val DESCRIPTION = "description"
}