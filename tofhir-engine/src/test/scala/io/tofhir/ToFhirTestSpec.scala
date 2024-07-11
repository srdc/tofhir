package io.tofhir

import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.execution.RunningJobRegistry
import io.tofhir.engine.mapping._
import io.tofhir.engine.mapping.context.{IMappingContextLoader, MappingContextLoader}
import io.tofhir.engine.mapping.schema.SchemaFolderLoader
import io.tofhir.engine.repository.mapping.{FhirMappingFolderRepository, IFhirMappingRepository}
import io.tofhir.engine.util.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors, OptionValues}

import java.io.FileWriter
import java.net.URI
import scala.io.Source

trait ToFhirTestSpec extends Matchers with OptionValues with Inside with Inspectors {

  val repositoryFolderUri: URI = getClass.getResource(ToFhirConfig.engineConfig.mappingRepositoryFolderPath).toURI
  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(repositoryFolderUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader

  val schemaRepositoryURI: URI = getClass.getResource(ToFhirConfig.engineConfig.schemaRepositoryFolderPath).toURI
  val schemaRepository = new SchemaFolderLoader(schemaRepositoryURI)

  val sparkSession: SparkSession = ToFhirConfig.sparkSession

  val runningJobRegistry: RunningJobRegistry = new RunningJobRegistry(sparkSession)

  /**
   * Copies the content of a resource file to given location in the context path.
   * @param path The path to the resource file
   * */
  def copyResourceFile(path: String): Unit = {
    // get the content of resource file
    val sourceData = Source.fromResource(path).mkString
    // get the location of resource file according to the context path
    val file = FileUtils.getPath(path).toAbsolutePath.toFile
    // create the parent directories if not exists
    file.getParentFile.mkdirs()
    // create the file
    val fw = new FileWriter(file)
    try fw.write(sourceData) finally fw.close()
  }
}
