package io.tofhir.server.repository.schema

import io.onfhir.api.validation.ProfileRestrictions

/**
 * Utility functions to help the management of the schema within the ISchemaRepository and relations with onFHIR ProfileRestrictions
 */
object SchemaManagementUtil {

  /**
   * Return an updated map for the ProfileRestrictions (of a StructureDefinition resource)
   *
   * @param profileRestrictionsMap The ProfileRestrictions map to be updated
   * @param schemaUrl URL of the schema to be updated (or to be added to the map)
   * @param schemaVersion Version of the schema to be updated (or to be added). A Schema can have multiple versions (so a schema URL can have multiple versions)
   * @param newProfileRestrictions The new ProfileRestrictions to be put into the map.
   * @return
   */
  def updateProfileRestrictionsMap(profileRestrictionsMap: Map[String, Map[String, ProfileRestrictions]], schemaUrl: String, schemaVersion: String, newProfileRestrictions: ProfileRestrictions): Map[String, Map[String, ProfileRestrictions]] = {
    profileRestrictionsMap.updated(
      schemaUrl,
      profileRestrictionsMap.get(schemaUrl) match {
        case Some(versionMap) =>
          // Update the existing inner map for the URL
          versionMap.updated(schemaVersion, newProfileRestrictions)
        case None =>
          // If no map exists for the URL, create a new inner map with the version
          Map(schemaVersion -> newProfileRestrictions)
      }
    )
  }
}
