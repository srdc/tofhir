package io.tofhir.server.service.project

import java.io.{File, FileWriter}

import com.typesafe.scalalogging.Logger
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.{Project, ProjectEditableFields}
import io.tofhir.engine.util.FileUtils
import io.tofhir.server.model.{AlreadyExists, ResourceNotFound}
import io.tofhir.server.util.FileOperations
import org.json4s._
import org.json4s.jackson.Serialization.writePretty

import scala.concurrent.Future

/**
 * Folder/Directory based project repository implementation.
 *
 * @param repositoryFolderPath root folder path to the repository
 */
class ProjectFolderRepository(repositoryFolderPath: String) extends IProjectRepository {

  private val logger: Logger = Logger(this.getClass)

  /**
   * Retrieve all Projects
   *
   * @return all projects in the repository
   */
  override def getAllProjects: Future[Seq[Project]] = {
    Future {
      getProjectsMetadata()
    }
  }

  /**
   * Save project to the repository.
   *
   * @param project project to be saved
   * @return the created project
   */
  override def createProject(project: Project): Future[Project] = {
    Future {
      val projects = getProjectsMetadata()
      // validate that there is no project with the given id and name
      if (projects.exists(p => p.id.contentEquals(project.id))) {
        throw AlreadyExists("Project already exists.", s"Project with id ${project.id} already exists in the repository.")
      }
      if (projects.exists(p => p.name.contentEquals(project.name))) {
        throw AlreadyExists("Project already exists.", s"Project with name ${project.name} already exists in the repository.")
      }
      // update projects metadata file by adding the new project
      updateProjectsMetadata(projects :+ project)
      // create a folder for the project
      new File(repositoryFolderPath + File.separatorChar + IProjectRepository.PROJECTS_FOLDER + File.separatorChar + FileUtils.getFileName(project.id, project.name)).mkdirs()
      project
    }
  }

  /**
   * Retrieve the project identified by its id.
   *
   * @param id id of the project
   * @return the project
   */
  override def getProject(id: String): Future[Option[Project]] = {
    Future {
      val projects = getProjectsMetadata()
      // validate that the project exists
      val project = projects.find(p => p.id.contentEquals(id))
      if (project.isEmpty)
        throw ResourceNotFound("Project does not exist.", s"Project $id not found ")
      project
    }
  }

  /**
   * Update the some fields of project in the repository.
   *
   * @param id    id of the project
   * @param patch patch to be applied to the project
   * @return
   */
  override def updateProject(id: String, patch: JObject): Future[Project] = {
    Future {
      val projects = getProjectsMetadata()
      // validate that the project exists
      val (project, remainingProjects) = projects.partition(p => p.id.contentEquals(id))
      if (project.isEmpty)
        throw ResourceNotFound("Project does not exist.", s"Project $id not found")
      // update the editable fields of project with new values
      val newDescription = (patch \ ProjectEditableFields.DESCRIPTION).extract[String]
      val updatedProject = project.head.copy(description = Some(newDescription))
      // update the projects metadata
      updateProjectsMetadata(remainingProjects :+ updatedProject)
      updatedProject
    }
  }

  /**
   * Delete the project from the repository.
   *
   * @param id id of the project
   * @return
   */
  override def removeProject(id: String): Future[Unit] = {
    Future {
      val projects = getProjectsMetadata()
      // validate that the project exists
      val (project, remainingProjects) = projects.partition(p => p.id.contentEquals(id))
      if (project.isEmpty)
        throw ResourceNotFound("Project does not exist.", s"Project $id not found")
      // update projects metadata with the remaining ones
      updateProjectsMetadata(remainingProjects)
      // remove the project folder
      org.apache.commons.io.FileUtils.deleteDirectory(new File(repositoryFolderPath + File.separatorChar + IProjectRepository.PROJECTS_FOLDER + File.separatorChar + FileUtils.getFileName(project.head.id, project.head.name)))
    }
  }

  /**
   * Returns the projects in the repository by reading them from the projects metadata file.
   *
   * @return projects in the repository
   * */
  private def getProjectsMetadata(): Seq[Project] = {
    val file = FileUtils.findFileByName(repositoryFolderPath + File.separatorChar, IProjectRepository.PROJECTS_JSON)
    file match {
      case Some(f) =>
        FileOperations.readJsonContent(f, classOf[Project])
      case None => {
        // when projects metadata file does not exist, create it
        logger.debug("There does not exist a metadata file for projects. Creating it...")
        new File(repositoryFolderPath + File.separatorChar, IProjectRepository.PROJECTS_JSON).createNewFile()
        Seq.empty
      }
    }
  }

  /**
   * Updates the projects metadata file with the given projects.
   *
   * @param projects projects
   * */
  private def updateProjectsMetadata(projects: Seq[Project]) = {
    val file = new File(repositoryFolderPath + File.separatorChar, IProjectRepository.PROJECTS_JSON)
    // when projects metadata file does not exist, create it
    if (!file.exists()) {
      logger.debug("There does not exist a metadata file for projects to update. Creating it...")
      file.createNewFile()
    }
    // write projects to the file
    val fw = new FileWriter(file)
    try fw.write(writePretty(projects)) finally fw.close()
  }
}
