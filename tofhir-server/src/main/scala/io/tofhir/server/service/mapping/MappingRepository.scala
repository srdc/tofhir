package io.tofhir.server.service.mapping

import io.onfhir.api.util.IOUtil
import io.onfhir.util.JsonFormatter._
import io.tofhir.engine.Execution.actorSystem.dispatcher
import io.tofhir.engine.model.{FhirMapping, FhirMappingException}
import io.tofhir.engine.util.FileUtils
import io.tofhir.engine.util.FileUtils.FileExtensions
import io.tofhir.server.model.{AlreadyExists, BadRequest, MappingFile, ResourceNotFound}
import org.apache.commons.io.FilenameUtils
import org.json4s._
import org.json4s.jackson.Serialization.writePretty

import java.io.{File, FileWriter}
import java.nio.charset.StandardCharsets
import scala.concurrent.Future
import scala.io.Source

/**
 * Folder/Directory based mapping repository implementation.
 *
 * @param mappingRepositoryFolderPath root folder path to the mapping repository
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
   * Save the mapping to the repository.
   *
   * @param directory  subfolder to save the mapping in
   * @param mapping mapping to save
   * @return
   */
  override def createMapping(directory: String, mapping: FhirMapping): Future[FhirMapping] = {
    Future {
      val fileName = mapping.name + FileExtensions.JSON.toString
      val file = new File(mappingRepositoryFolderPath + File.separatorChar + directory + File.separatorChar + fileName)
      if (file.exists()) {
        throw AlreadyExists("Mapping already exists.", s"Mapping with name ${mapping.name} already exists in the repository.")
      }
      file.createNewFile()
      val fw = new FileWriter(file)
      fw.write(writePretty(mapping))
      fw.close()
      mapping
    }
  }

  /**
   * Retrieve the mapping identified by its directory and name.
   *
   * @param directory subfolder the mapping is in
   * @param name name of the mapping
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

  /**
   * Update the mapping in the repository.
   * @param directory subfolder the mapping is in
   * @param name name of the mapping
   * @param fhirMapping mapping to update
   * @return
   */
  override def updateMapping(directory: String, name: String, fhirMapping: FhirMapping): Future[FhirMapping] = {
    Future {
      if (fhirMapping.name != name) throw BadRequest("Mapping name mismatch", "Name of the mapping in the request body does not match the name in the URL.")
      val fileName = name + FileExtensions.JSON.toString
      val file = FileUtils.findFileByName(mappingRepositoryFolderPath + File.separatorChar +  directory, fileName)
      file match {
        case Some(f) =>
          val fw = new FileWriter(f)
          fw.write(writePretty(fhirMapping))
          fw.close()
          fhirMapping
        case None => throw ResourceNotFound("Mapping does not exist." ,s"Mapping file $fileName not found in directory $directory")
      }
    }
  }

  /**
   * Delete the mapping from the repository.
   *
   * @param directory subfolder the mapping is in
   * @param name      name of the mapping
   * @return
   */
  override def removeMapping(directory: String, name: String): Future[Unit] = {
    Future {
      val fileName = name + FileExtensions.JSON.toString
      val file = FileUtils.findFileByName(mappingRepositoryFolderPath + File.separatorChar +  directory, fileName)
      file match {
        case Some(f) =>
          f.delete()
        case None => throw ResourceNotFound("Mapping does not exist." ,s"Mapping file $fileName not found in directory $directory")
      }
    }
  }

}
