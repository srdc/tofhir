package io.tofhir.server.endpoint

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.server.common.model.{ResourceNotFound, ToFhirRestCall}
import io.tofhir.server.endpoint.ProjectEndpoint.SEGMENT_PROJECTS
import io.onfhir.definitions.common.model.Json4sSupport._
import io.tofhir.server.model.Project
import io.tofhir.server.repository.job.IJobRepository
import io.tofhir.server.repository.mapping.IMappingRepository
import io.tofhir.server.repository.mappingContext.IMappingContextRepository
import io.tofhir.server.repository.project.IProjectRepository
import io.tofhir.server.repository.schema.ISchemaRepository
import io.tofhir.server.service.ProjectService
import org.json4s.JObject

import scala.concurrent.Future

/**
 * Endpoints to manage projects.
 * */
class ProjectEndpoint(schemaRepository: ISchemaRepository,
                      mappingRepository: IMappingRepository,
                      jobRepository: IJobRepository,
                      mappingContextRepository: IMappingContextRepository,
                      projectRepository: IProjectRepository) extends LazyLogging {

  val service: ProjectService = new ProjectService(projectRepository, jobRepository, mappingRepository, mappingContextRepository, schemaRepository)
  val schemaDefinitionEndpoint: SchemaDefinitionEndpoint = new SchemaDefinitionEndpoint(schemaRepository, mappingRepository)
  val mappingEndpoint: MappingEndpoint = new MappingEndpoint(mappingRepository, jobRepository)
  val jobEndpoint: JobEndpoint = new JobEndpoint(jobRepository, mappingRepository, schemaRepository)
  val mappingContextEndpoint: MappingContextEndpoint = new MappingContextEndpoint(mappingContextRepository)

  def route(request: ToFhirRestCall): Route = {
    pathPrefix(SEGMENT_PROJECTS) {
      // operations on all projects
      pathEndOrSingleSlash {
        createProjectRoute() ~ getProjectsRoute
      } ~ // operations on individual projects
        pathPrefix(Segment) { projectId: String =>
          getProjectRoute(projectId) ~ patchProjectRoute(projectId) ~ deleteProjectRoute(projectId) ~ {
            val projectExists: Future[Option[Project]] = service.getProject(projectId)
            onSuccess(projectExists) {
              case None => complete {
                StatusCodes.NotFound -> {
                  throw ResourceNotFound("Project not found", s"Project with id $projectId not found")
                }
              }
              case Some(_) => {
                request.projectId = Some(projectId)
                schemaDefinitionEndpoint.route(request) ~ mappingEndpoint.route(request) ~ jobEndpoint.route(request) ~ mappingContextEndpoint.route(request)
              }
            }
          }
        }
    }
  }

  /**
   * Route to create a project
   *
   * @return
   */
  private def createProjectRoute(): Route = {
    post {
      entity(as[Project]) { project =>
        complete {
          service.createProject(project) map { createdDefinition =>
            StatusCodes.Created -> createdDefinition
          }
        }
      }
    }
  }

  /**
   * Route to get all projects
   *
   * @return
   */
  private def getProjectsRoute: Route = {
    get {
      complete {
        service.getAllProjects
      }
    }
  }

  /**
   * Route to get a project
   *
   * @param projectId id of project
   * @return
   */
  private def getProjectRoute(projectId: String): Route = {
    pathEndOrSingleSlash {
      get {
        complete {
          service.getProject(projectId) map {
            case Some(project) => StatusCodes.OK -> project
            case None => StatusCodes.NotFound -> {
              throw ResourceNotFound("Project not found", s"Project with id $projectId not found")
            }
          }
        }
      }
    }
  }

  /**
   * Route to patch a project
   *
   * @param projectId id of project
   * @return
   */
  private def patchProjectRoute(projectId: String): Route = {
    patch {
      entity(as[JObject]) { project =>
        complete {
          service.updateProject(projectId, project) map { updatedProject =>
            StatusCodes.OK -> updatedProject
          }
        }
      }
    }
  }

  /**
   * Route to delete a project
   *
   * @param projectId id of project to be deleted
   * @return
   */
  private def deleteProjectRoute(projectId: String): Route = {
    pathEndOrSingleSlash {
      delete {
        complete {
          service.removeProject(projectId) map { _ =>
            StatusCodes.NoContent
          }
        }
      }
    }
  }
}

object ProjectEndpoint {
  val SEGMENT_PROJECTS = "projects"
}



