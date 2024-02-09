package io.tofhir

import akka.actor.ActorSystem
import io.tofhir.engine.config.ToFhirConfig
import io.tofhir.engine.execution.RunningJobRegistry
import io.tofhir.engine.mapping._
import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Inside, Inspectors, OptionValues}

import java.net.URI

trait ToFhirTestSpec extends Matchers with OptionValues with Inside with Inspectors {

  val repositoryFolderUri: URI = getClass.getResource(ToFhirConfig.engineConfig.mappingRepositoryFolderPath).toURI
  val mappingRepository: IFhirMappingRepository = new FhirMappingFolderRepository(repositoryFolderUri)

  val contextLoader: IMappingContextLoader = new MappingContextLoader(mappingRepository)

  val schemaRepositoryURI: URI = getClass.getResource(ToFhirConfig.engineConfig.schemaRepositoryFolderPath).toURI
  val schemaRepository = new SchemaFolderLoader(schemaRepositoryURI)

  val sparkSession: SparkSession = ToFhirConfig.sparkSession

  val runningJobRegistry: RunningJobRegistry = new RunningJobRegistry(sparkSession)

  implicit val actorSystem: ActorSystem = ActorSystem("toFhirEngineTest")
}
