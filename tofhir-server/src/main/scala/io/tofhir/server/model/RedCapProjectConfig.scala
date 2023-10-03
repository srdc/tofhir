package io.tofhir.server.model

/**
 * The configuration of tofhir-redcap.
 *
 * @param apiURL   The URL of REDCap API
 * @param projects The configurations of REDCap projects
 * */
case class RedCapConfig(apiURL: String, projects: Seq[RedCapProjectConfig])

/**
 * The configuration of a REDCap project.
 *
 * @param id    Unique identifier of REDCap project
 * @param token The token for REDCap project
 * */
case class RedCapProjectConfig(id: String, token: String)