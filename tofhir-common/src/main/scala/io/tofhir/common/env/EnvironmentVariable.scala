package io.tofhir.common.env

object EnvironmentVariable extends Enumeration {
  type EnvironmentVariable = Value
  final val FHIR_REPO_URL = Value("FHIR_REPO_URL")
  final val DATA_FOLDER_PATH= Value("DATA_FOLDER_PATH")
}
