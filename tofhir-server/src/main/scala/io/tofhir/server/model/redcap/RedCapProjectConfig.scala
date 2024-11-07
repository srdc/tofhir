package io.tofhir.server.model.redcap

/**
 * Represents the configuration details for a REDCap project.
 *
 * @param id            The unique identifier of the REDCap project.
 * @param token         The API token used for accessing the REDCap project.
 * @param recordIdField The field name used as the unique record identifier within the project. Defaults to "record_id".
 */
case class RedCapProjectConfig(id: String, token: String, recordIdField: String = "record_id")
