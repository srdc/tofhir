package io.tofhir.engine.env

import io.tofhir.engine.model.{FhirMappingJob, FhirMappingTask, FhirRepositorySinkSettings, FhirSinkSettings, FileSystemSourceSettings, KafkaSource, KafkaSourceSettings, MappingJobSourceSettings}

/**
 * Provide functions to find and resolve the environment variables used within FhirMappingJob definitions.
 * An ENV_VAR is given as ${ENV_VAR} within the MappingJob definition.
 */
object EnvironmentVariableResolver {

  /**
   * Reads the environment variables defined in the EnvironmentVariable object.
   *
   * @return A Map[String, String] containing the environment variable names and their values.
   */
  def getEnvironmentVariables: Map[String, String] = {
    EnvironmentVariable.values
      .map(_.toString) // Convert Enumeration values to String (names of the variables)
      .flatMap { envVar =>
        sys.env.get(envVar).map(envVar -> _) // Get the environment variable value if it exists
      }
      .toMap
  }

  /**
   * Resolves the environment variables from a given String which is a file content of a FhirMappingJob definition.
   *
   * @param fileContent The file content potentially containing placeholders for environment variables.
   * @return The file content with all recognized environment variables resolved.
   */
  def resolveFileContent(fileContent: String): String = {
    EnvironmentVariable.values.foldLeft(fileContent) { (updatedContent, envVar) =>
      val placeholder = "\\$\\{" + envVar.toString + "\\}"
      sys.env.get(envVar.toString) match {
        case Some(envValue) =>
          updatedContent.replaceAll(placeholder, envValue)
        case None =>
          updatedContent // Leave the content unchanged if the environment variable is not set
      }
    }
  }

  /**
   * Resolves all environment variable placeholders in a FhirMappingJob instance.
   *
   * @param job FhirMappingJob instance.
   * @return A new FhirMappingJob with resolved values.
   */
  def resolveFhirMappingJob(job: FhirMappingJob): FhirMappingJob = {
    job.copy(
      sourceSettings = resolveSourceSettings(job.sourceSettings),
      sinkSettings = resolveSinkSettings(job.sinkSettings),
      mappings = resolveMappings(job.mappings)
    )
  }

  /**
   * Resolves environment variables in MappingJobSourceSettings.
   *
   * @param sourceSettings A map of source settings.
   * @return The map with resolved settings.
   */
  private def resolveSourceSettings(sourceSettings: Map[String, MappingJobSourceSettings]): Map[String, MappingJobSourceSettings] = {
    sourceSettings.map { case (key, value) =>
      key -> (value match {
        case fs: FileSystemSourceSettings =>
          fs.copy(dataFolderPath = resolveEnvironmentVariables(fs.dataFolderPath))
        case ks: KafkaSourceSettings if ks.asRedCap =>
          ks.copy(sourceUri = resolveEnvironmentVariables(ks.sourceUri))
        case other => other // No changes for other types
      })
    }
  }

  /**
   * Resolves environment variables in mappings.
   *
   * @param mappings The list of mapping tasks
   * @return The resolved mappings
   */
  private def resolveMappings(mappings: Seq[FhirMappingTask]): Seq[FhirMappingTask] = {
    mappings.map { mapping =>
      val updatedSourceBinding = mapping.sourceBinding.map { case (key, source) =>
        key -> (source match {
          case kafkaSource: KafkaSource =>
            kafkaSource.copy(topicName = resolveEnvironmentVariables(kafkaSource.topicName))
          case other => other // No changes for other source types
        })
      }

      mapping.copy(sourceBinding = updatedSourceBinding)
    }
  }

  /**
   * Resolves environment variables in FhirRepositorySinkSettings.
   *
   * @param sinkSettings FhirSinkSettings instance.
   * @return FhirSinkSettings with resolved fhirRepoUrl, if applicable.
   */
  private def resolveSinkSettings(sinkSettings: FhirSinkSettings): FhirSinkSettings = {
    sinkSettings match {
      case fr: FhirRepositorySinkSettings =>
        fr.copy(fhirRepoUrl = resolveEnvironmentVariables(fr.fhirRepoUrl))
      case other => other // No changes for other types
    }
  }

  /**
   * Resolves placeholders in the format ${ENV_VAR} with their corresponding values
   * from the system environment variables, but only if the variables are part
   * of the EnvironmentVariable enumeration.
   *
   * @param value The string potentially containing placeholders.
   * @return The resolved string with placeholders replaced.
   */
  private def resolveEnvironmentVariables(value: String): String = {
    val pattern = """\$\{([A-Z_]+)\}""".r

    pattern.replaceAllIn(value, m => {
      val envVar = m.group(1)
      if (EnvironmentVariable.values.exists(_.toString == envVar)) {
        sys.env.getOrElse(envVar, {
          throw new RuntimeException(s"Environment variable $envVar is not set.")
        })
      } else {
        throw new RuntimeException(s"Environment variable $envVar is not recognized in EnvironmentVariable enumeration.")
      }
    })
  }

}
