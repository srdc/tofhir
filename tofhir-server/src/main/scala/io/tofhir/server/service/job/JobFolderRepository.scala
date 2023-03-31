package io.tofhir.server.service.job

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import com.typesafe.scalalogging.Logger
import io.onfhir.api.util.IOUtil
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.ToFhirEngine
import io.tofhir.engine.config.{ErrorHandlingType, ToFhirConfig}
import io.tofhir.engine.mapping.FhirMappingJobManager
import io.tofhir.engine.model.{FhirMappingJob, FhirMappingJobExecution, FhirMappingResult, FhirMappingTask, FileSystemSource, FileSystemSourceSettings}
import io.tofhir.engine.util.FhirMappingJobFormatter.formats
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.model.{AlreadyExists, BadRequest, FhirMappingTaskTest, Project, ResourceNotFound}
import io.tofhir.server.service.project.ProjectFolderRepository
import io.tofhir.server.util.MappingTestUtil
import org.json4s.jackson.Serialization.writePretty

import scala.collection.mutable
import scala.concurrent.Future
import scala.io.Source

class JobFolderRepository(jobRepositoryFolderPath: String, projectFolderRepository: ProjectFolderRepository) extends IJobRepository {

  private val logger: Logger = Logger(this.getClass)
  // project id -> mapping job id -> mapping job
  private val jobDefinitions: mutable.Map[String, mutable.Map[String, FhirMappingJob]] = initMap(jobRepositoryFolderPath)

  val toFhirEngine = new ToFhirEngine(ToFhirConfig.sparkAppName, ToFhirConfig.sparkMaster,
    ToFhirConfig.engineConfig.mappingRepositoryFolderPath,
    ToFhirConfig.engineConfig.schemaRepositoryFolderPath)

  val fhirMappingJobManager =
    new FhirMappingJobManager(
      toFhirEngine.mappingRepository,
      toFhirEngine.contextLoader,
      toFhirEngine.schemaLoader,
      toFhirEngine.sparkSession,
      ErrorHandlingType.CONTINUE
    )

  /**
   * Returns the mappings managed by this repository
   *
   * @return
   */
  def getCachedMappingsJobs(): mutable.Map[String, mutable.Map[String, FhirMappingJob]] = {
    jobDefinitions
  }


  /**
   * Retrieve all jobs
   *
   * @param projectId project id the jobs belong to
   * @return
   */
  override def getAllJobs(projectId: String): Future[Seq[FhirMappingJob]] = {
    Future {
      if (jobDefinitions.contains(projectId)) {
        jobDefinitions(projectId).values.toSeq
      } else {
        Seq.empty
      }
    }
  }

  /**
   * Save the job to the repository.
   *
   * @param projectId project id the job belongs to
   * @param job       job to save
   * @return
   */
  override def createJob(projectId: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    Future {
      if (jobDefinitions.contains(projectId) && jobDefinitions(projectId).contains(job.id)) {
        throw AlreadyExists("Fhir mapping job already exists.", s"A job definition with id ${job.id} already exists in the job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
      }
      // Write to the repository as a new file
      getFileForJob(projectId, job).map(file => {
        val fw = new FileWriter(file)
        fw.write(writePretty(job))
        fw.close()
      })
      projectFolderRepository.addJob(projectId, job)
      jobDefinitions.getOrElseUpdate(projectId, mutable.Map.empty).put(job.id, job)
      job
    }
  }

  /**
   * Get the job by its id
   *
   * @param projectId project id the job belongs to
   * @param id        job id
   * @return
   */
override def getJob(projectId: String, id: String): Future[Option[FhirMappingJob]] = {
  Future {
    jobDefinitions(projectId).get(id)
  }
}

  /**
   * Update the job in the repository
   *
   * @param projectId project id the job belongs to
   * @param id        job id
   * @param job       job to save
   * @return
   */
  override def putJob(projectId: String, id: String, job: FhirMappingJob): Future[FhirMappingJob] = {
    Future {
      if (!id.equals(job.id)) {
        throw BadRequest("Job definition is not valid.", s"Identifier of the job definition: ${job.id} does not match with explicit id: $id")
      }
      if (!jobDefinitions.contains(projectId) || !jobDefinitions(projectId).contains(id)) {
        throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $id does not exists in the mapping job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
      }
      // update the job in the repository
      getFileForJob(projectId, job).map(file => {
        val fw = new FileWriter(file)
        fw.write(writePretty(job))
        fw.close()
      })
      // update the mapping job in the map
      jobDefinitions(projectId).put(id, job)
      // update the job in the project
      projectFolderRepository.updateJob(projectId, job)
      job
    }

  }

  /**
   * Delete the job from the repository
   *
   * @param projectId project id the job belongs to
   * @param id        job id
   * @return
   */
  override def deleteJob(projectId: String, id: String): Future[Unit] = {
    Future {
      if (!jobDefinitions.contains(projectId) || !jobDefinitions(projectId).contains(id)) {
        throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $id does not exists in the mapping job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
      }
      // delete the mapping job from the repository
      getFileForJob(projectId, jobDefinitions(projectId)(id)).map(file => {
        file.delete()
      })
      // delete the mapping job from the map
      jobDefinitions(projectId).remove(id)
      // delete the job from the project
      projectFolderRepository.deleteJob(projectId, id)
    }
  }

  /**
   * Run the job for the specified mapping tasks. If no mapping tasks are specified, run the mapping job for all
   * of them.
   * @param projectId project id the job belongs to
   * @param id        job id
   * @param mappingUrls the urls of mapping tasks to be executed
   * @return
   */
  override def runJob(projectId: String, id: String, mappingUrls: Option[Seq[String]]=None): Future[Future[Unit]] = {
    Future {
      if (!jobDefinitions.contains(projectId) || !jobDefinitions(projectId).contains(id)) {
        throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $id does not exists in the mapping job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
      }
      val mappingJob: FhirMappingJob = jobDefinitions(projectId)(id)
      // get the list of mapping task to be executed
      val mappingTasks = mappingUrls match {
        case Some(urls) => mappingJob.mappings.filter(m => urls.contains(m.mappingRef))
        case None => mappingJob.mappings
      }
      // create execution
      val mappingJobExecution = FhirMappingJobExecution(jobId = mappingJob.id, projectId = projectId, mappingTasks = mappingTasks)
      if (mappingJob.sourceSettings.exists(_._2.asStream)) {
        Future { // TODO we lose the ability to stop the streaming job
          val streamingQuery =
            fhirMappingJobManager
              .startMappingJobStream(
                mappingJobExecution,
                sourceSettings = mappingJob.sourceSettings,
                sinkSettings = mappingJob.sinkSettings,
                terminologyServiceSettings = mappingJob.terminologyServiceSettings,
                identityServiceSettings = mappingJob.getIdentityServiceSettings()
              )
          streamingQuery.awaitTermination()
        }
      } else {
        fhirMappingJobManager
          .executeMappingJob(
            mappingJobExecution = mappingJobExecution,
            sourceSettings = mappingJob.sourceSettings,
            sinkSettings = mappingJob.sinkSettings,
            terminologyServiceSettings = mappingJob.terminologyServiceSettings,
            identityServiceSettings = mappingJob.getIdentityServiceSettings()
          )
      }
    }
  }

  /**
   * Tests the given mapping task by running it with mapping job configurations (i.e. source data configurations) and
   * returns its results
   *
   * @param projectId project id the job belongs to
   * @param id job id
   * @param mappingTaskTest mappingTaskTest object to be tested
   * @return
   */
  override def testMappingWithJob(projectId: String, id: String, mappingTaskTest: FhirMappingTaskTest): Future[Future[Seq[FhirMappingResult]]] = {
    Future {
      if (!jobDefinitions.contains(projectId) || !jobDefinitions(projectId).contains(id)) {
        throw ResourceNotFound("Mapping job does not exists.", s"A mapping job with id $id does not exists in the mapping job repository at ${FileUtils.getPath(jobRepositoryFolderPath).toAbsolutePath.toString}")
      }
      val mappingJob: FhirMappingJob = jobDefinitions(projectId)(id)
      // create test files and return updated fhir mapping task
      val updatedMappingTaskTest = MappingTestUtil.createTaskWithSelection(mappingJob, mappingTaskTest)
      fhirMappingJobManager.executeMappingTaskAndReturn(
        mappingJobExecution = FhirMappingJobExecution(jobId = mappingJob.id, projectId = projectId, mappingTasks = Seq(updatedMappingTaskTest.fhirMappingTask)),
        sourceSettings = mappingJob.sourceSettings,
        terminologyServiceSettings = mappingJob.terminologyServiceSettings,
        identityServiceSettings = mappingJob.getIdentityServiceSettings()
      ) map { results =>
        // delete the test files after the test results are returned
        MappingTestUtil.deleteCreatedTestFiles(mappingJob, updatedMappingTaskTest)
        results
      }
    }
  }
  /**
   * Get the mapping job file for the given project id and job id
 *
   * @param projectId
   * @param fhirMapping
   * @return
   */
  private def getFileForJob(projectId: String, fhirMapping: FhirMappingJob): Future[File] = {
    val projectFuture: Future[Option[Project]] = projectFolderRepository.getProject(projectId)
    projectFuture.map(project => {
      val file: File = FileUtils.getPath(jobRepositoryFolderPath, project.get.id, getFileName(fhirMapping.id)).toFile
      // If the project folder does not exist, create it
      if (!file.getParentFile.exists()) {
        file.getParentFile.mkdir()
      }
      file
    })
  }

  /**
   * Get the file name of the given job
   * @param jobId job id
   * @return
   */
  private def getFileName(jobId: String): String = {
    s"$jobId${FileExtensions.JSON}"
  }

  /**
   * Initialize the job definitions from the given folder
   * @param jobRepositoryFolderPath folder path to the job repository
   * @return
   */
  private def initMap(jobRepositoryFolderPath: String): mutable.Map[String, mutable.Map[String, FhirMappingJob]] = {
    val map = mutable.Map.empty[String, mutable.Map[String, FhirMappingJob]]
    val folder = FileUtils.getPath(jobRepositoryFolderPath).toFile
    if (!folder.exists()) {
      folder.mkdirs()
    }
    var directories = Seq.empty[File]
    directories = folder.listFiles.filter(_.isDirectory).toSeq
    directories.foreach { projectDirectory =>
      // job-id -> FhirMappingJob
      val fhirJobMap: mutable.Map[String, FhirMappingJob] = mutable.Map.empty
      val files = IOUtil.getFilesFromFolder(projectDirectory, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
      files.map { file =>
        val source = Source.fromFile(file, StandardCharsets.UTF_8.name()) // read the JSON file
        val fileContent = try source.mkString finally source.close()
        val job = fileContent.parseJson.extract[FhirMappingJob]
        fhirJobMap.put(job.id, job)
      }
      map.put(projectDirectory.getName, fhirJobMap)
    }
    map
  }
}
