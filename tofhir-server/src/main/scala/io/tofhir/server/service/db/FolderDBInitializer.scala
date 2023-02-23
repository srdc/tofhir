//package io.tofhir.server.service.db
//
//import java.io.{File, FileWriter}
//
//import com.typesafe.scalalogging.Logger
//import io.onfhir.util.JsonFormatter.formats
//import io.tofhir.engine.config.ToFhirEngineConfig
//import io.tofhir.server.model.{Project, SchemaDefinition}
//import io.tofhir.server.service.project.ProjectFolderRepository
//import io.tofhir.server.service.schema.SchemaFolderRepository
//import org.json4s.jackson.Serialization.writePretty
//
//import scala.collection.mutable
//
///**
// * Folder/Directory based database initializer implementation.
// * */
//class FolderDBInitializer(toFhirEngineConfig: ToFhirEngineConfig, schemeFolderRepository: SchemaFolderRepository) {
//
//  private val logger: Logger = Logger(this.getClass)
//
//  /**
//   * Creates repository directory [[ToFhirEngineConfig.toFhirDbFolderPath]] if it does not exist.
//   * Further, it creates [[ProjectFolderRepository.PROJECTS_JSON]] file if it does not exist but mapping, jobs and schema folders do.
//   * It simply populates the json file using the content of other data folders. It utilizes respective folder-based repositories
//   * and assumes that those repositories are already initialized with those existing resources.
//   */
//  def initialize(): Unit = {
//    // create repository directory if it does not exist
//    val repositoryDirectory = new File(toFhirEngineConfig.toFhirDbFolderPath)
//    if(!repositoryDirectory.exists()){
//      repositoryDirectory.mkdirs()
//    }
//
//    // check whether projects metadata file exists
//    val file = new File(toFhirEngineConfig.toFhirDbFolderPath + File.separatorChar, ProjectFolderRepository.PROJECTS_JSON)
//    if (file.exists()) {
//      logger.debug("Metadata file for projects exists. Skipping database initialization.")
//    }
//    else {
//      // Map keeping the projects. It uses the project name as a key.
//      val projects: mutable.Map[String, Project] = mutable.Map.empty
//      /*// read mapping jobs folder
//      val projectsInMappingJobsFolder = FileUtils.getPath(toFhirEngineConfig.repositoryRootPath, toFhirEngineConfig.mappingJobFileContextPath).toFile
//      if (projectsInMappingJobsFolder.exists()) {
//        projectsInMappingJobsFolder.listFiles().foreach(file => {
//          var project = getProject(projects, file.getName)
//          project = project ~ ("mappingJobs" -> JArray(file.listFiles().map(f => JObject("id" -> JString(UUID.randomUUID().toString), "path" -> JString(f.getName))).toList))
//          projects += (file.getName -> project)
//        })
//      }*/
//
//      // Populate schemas TODO apply similar logic for mappings and jobs
//      val schemas: mutable.Map[String, mutable.Map[String, SchemaDefinition]] = schemeFolderRepository.getCachedSchemas()
//      schemas.foreach(projectIdAndSchemas => {
//        val projectId: String = projectIdAndSchemas._1
//        val project: Project = projects.getOrElse(projectId, Project(projectId, projectId, None))
//        project.copy(schemas = projectIdAndSchemas._2.values.map(_.copyAsMetadata()).toSeq)
//        projects.put(projectId, project.copy(schemas = projectIdAndSchemas._2.values.map(_.copyAsMetadata()).toSeq))
//      })
//      /*val projectsInSchemasFolder = FileUtils.getPath(toFhirEngineConfig.repositoryRootPath, toFhirEngineConfig.schemaRepositoryFolderPath).toFile
//      if (projectsInSchemasFolder.exists()) {
//        projectsInSchemasFolder.listFiles().foreach(file => {
//          var project = getProject(projects, file.getName)
//          project = project ~ ("schemas" -> JArray(file.listFiles().map(f => {
//            val schema = FileOperations.readJsonContentAsObject(f)
//            JObject("id" -> JString(UUID.randomUUID().toString), "path" -> JString(f.getName), "url" -> schema \ "url", "type" -> schema \ "type")
//          }).toList))
//          projects += (file.getName -> project)
//        })
//      }*/
//      // read mappings folder which can include: Mapping definitions, concept maps and unit conversions
//      /*val projectsInMappingsFolder = FileUtils.getPath(toFhirEngineConfig.repositoryRootPath, toFhirEngineConfig.mappingRepositoryFolderPath).toFile
//      if (projectsInMappingsFolder.exists()) {
//        projectsInMappingsFolder.listFiles().foreach(file => {
//          var project = getProject(projects, file.getName)
//          // read mappings
//          project = project ~ ("mappings" -> JArray(file.listFiles().filter(_.getName.endsWith(".json")).map(f => {
//            val schema = FileOperations.readJsonContentAsObject(f)
//            JObject("id" -> JString(UUID.randomUUID().toString), "path" -> JString(f.getName), "url" -> schema \ "url")
//          }).toList))
//          // read concept maps
//          val csvFiles = file.listFiles().filter(f => f.getName.endsWith(".csv"))
//          var contextConceptMaps: Seq[JObject] = Seq.empty
//          contextConceptMaps = contextConceptMaps ++ (csvFiles.filter(f => !FileOperations.isUnitConversionFile(f))
//            .map(f => JObject("id" -> JString(UUID.randomUUID().toString), "path" -> JString(f.getName), "category" -> JString(FhirMappingContextCategories.CONCEPT_MAP))).toList)
//          // read unit conversion concept maps
//          contextConceptMaps = contextConceptMaps ++ (csvFiles.filter(f => FileOperations.isUnitConversionFile(f))
//            .map(f => JObject("id" -> JString(UUID.randomUUID().toString), "path" -> JString(f.getName), "category" -> JString(FhirMappingContextCategories.UNIT_CONVERSION_FUNCTIONS))).toList)
//
//          project = project ~ ("contextConceptMaps" -> JArray(contextConceptMaps.toList))
//          projects += (file.getName -> project)
//        })
//      }*/
//
//      // create projects metadata file if there are some projects
//      val projectsMetadata = projects.values
//      if (projectsMetadata.nonEmpty) {
//        // create projects metadata file
//        val file = new File(toFhirEngineConfig.toFhirDbFolderPath + File.separatorChar, ProjectFolderRepository.PROJECTS_JSON)
//        file.createNewFile()
//        // write projects to the file
//        val fw = new FileWriter(file)
//        try fw.write(writePretty(projectsMetadata)) finally fw.close()
//        logger.debug("Metadata file for projects created in the initialization of database.")
//      } else {
//        logger.debug("No project folders to initialize database. Skipping database initialization.")
//      }
//    }
//  }
//
//  /**
//   * Returns the Project associated with the given project. If the project does not exist in the map,
//   * it creates a new one and returns it.
//   *
//   * @param projects  Map keeping the list of projects. Project name is used as a key in the map.
//   * @param projectId Identifier of the project
//   * @return project as JObject
//   * */
//  private def getProject(projects: Map[String, Project], projectId: String): Project = {
//    if (projects.contains(projectId)) {
//      projects(projectId)
//    } else {
//      Project(projectId, projectId, None)
//    }
//  }
//}
