package io.tofhir.server.model

import java.util.UUID

/**
 * Local Terminology Service
 *
 * @param id unique id
 * @param name name of the terminology
 * @param description description of the terminology
 * @param folderPath folder path of the terminology
 */
case class LocalTerminology(id: String = UUID.randomUUID().toString, name: String, description: String, folderPath: String) {
  def withId(id: String): LocalTerminology = {
    this.copy(id = id)
  }
}
