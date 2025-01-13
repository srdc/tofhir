package io.tofhir.engine.env

object EnvironmentVariable extends Enumeration {
  type EnvironmentVariable = Value
  final val FHIR_REPO_URL = Value("FHIR_REPO_URL")
  final val DATA_FOLDER_PATH= Value("DATA_FOLDER_PATH")
  final val SOURCE_URL = Value("SOURCE_URL")
  final val REDCAP_PROJECT_ID= Value("REDCAP_PROJECT_ID")
}
