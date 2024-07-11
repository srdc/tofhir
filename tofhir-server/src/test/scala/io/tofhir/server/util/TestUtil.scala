package io.tofhir.server.util

import io.tofhir.engine.config.ToFhirEngineConfig
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.repository.project.ProjectFolderRepository
import org.json4s.JArray

object TestUtil {

  /**
   * Reads the project json file as a [[JArray]]
   *
   * @param engineConfig
   * @return
   */
  def getProjectJsonFile(engineConfig: ToFhirEngineConfig): JArray = {
    FileOperations.readFileIntoJson(FileUtils.getPath(ProjectFolderRepository.PROJECTS_JSON).toFile).asInstanceOf[JArray]
  }
}
