package io.tofhir.server.service.mapping

import io.onfhir.api.util.IOUtil
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.{FhirMapping, FhirMappingException}
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.model.MappingFile
import org.apache.commons.io.FilenameUtils

import java.io.File
import java.nio.charset.StandardCharsets
import scala.concurrent.Future
import scala.io.Source

/**
 * Folder/Directory based mapping repository implementation.
 *
 * @param mappingRepositoryFolderPath
 */
class MappingRepository(mappingRepositoryFolderPath: String) extends IMappingRepository {

  /**
   * Retrieve the metadata of all MappingFile (only url, type and name fields are populated)
   *
   * @return
   */
  override def getAllMappingMetadata(withSubFolder: Option[String]): Future[Seq[MappingFile]] = {
    Future {
      val repoDirectory = FileUtils.getPath(mappingRepositoryFolderPath).toFile
      var directories = Seq.empty[File]
      try {
        directories = repoDirectory.listFiles.filter(_.isDirectory).toSeq
        if (withSubFolder.isDefined) {
          directories = directories.filter(_.getName == withSubFolder.get)
        }
      } catch {
        case e: Throwable => throw FhirMappingException(s"Given folder for the mapping repository is not valid.", e)
      }

      directories.flatMap { directory =>
        val files = IOUtil.getFilesFromFolder(directory, withExtension = Some(FileExtensions.JSON.toString), recursively = Some(true))
        files
          .map { file =>
            MappingFile(FilenameUtils.removeExtension(file.getName), directory.getName)
          }
      }
    }
  }

  /**
   * Retrieve the mapping identified by its directory and name.
   *
   * @param directory
   * @param name
   * @return
   */
  override def getMappingByName(directory: String, name: String): Future[Option[FhirMapping]] = {
    Future {
      val fileName = name + FileExtensions.JSON.toString
      val file = FileUtils.findFileByName(mappingRepositoryFolderPath + File.separatorChar +  directory, fileName)
      file match {
        case Some(f) =>
          val source = Source.fromFile(f, StandardCharsets.UTF_8.name()) // read the JSON file
          val fileContent = try source.mkString finally source.close()
          val fhirMapping = fileContent.parseJson.extractOpt[FhirMapping]
          fhirMapping
        case None => None
      }
    }
  }

}
