package io.tofhir.server.service

import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.model.{RedCapConfig, RedCapProjectConfig}

import java.io.{File, FileWriter}
import javax.ws.rs.InternalServerErrorException
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

class RedCapIntegrationModuleService() extends LazyLogging {

  /**
   * tofhir-redcap environment variables
   * */
  val REDCAP_PROJECT_IDS = "REDCAP_PROJECT_IDS="
  val REDCAP_PROJECT_TOKENS = "REDCAP_PROJECT_TOKENS="
  val REDCAP_URL = "REDCAP_URL="

  /**
   * Returns the configuration of tofhir-redcap.
   *
   * @return the configuration of tofhir-redcap.
   * */
  def getToFhirRedcapConfig: Future[RedCapConfig] = {
    Future {
      // read redcap/.env file
      val source = Source.fromResource("redcap/.env")
      try {
        // extract configurations
        var redcapUrl = ""
        var projectIds: Seq[String] = Seq.empty
        var tokens: Seq[String] = Seq.empty
        source.getLines().toSeq
          .foreach(line => {
            if (line.startsWith(REDCAP_PROJECT_IDS))
              projectIds = line.substring(REDCAP_PROJECT_IDS.length).split(",").toSeq
            else if (line.startsWith(REDCAP_PROJECT_TOKENS))
              tokens = line.substring(REDCAP_PROJECT_TOKENS.length).split(",").toSeq
            else
              redcapUrl = line.substring(REDCAP_URL.length)
          })
        // create RedCapConfig model
        RedCapConfig(apiURL = redcapUrl, projects = projectIds.zipWithIndex.map(projectIdWithIndex => {
          RedCapProjectConfig(id = projectIdWithIndex._1, token = tokens.lift(projectIdWithIndex._2).get)
        }))
      } catch {
        case e: Throwable => throw new InternalServerErrorException("Error while reading tofhir-redcap configuration", e)
      } finally source.close()
    }
  }

  /**
   * Starts tofhir-redcap with the given configuration.
   *
   * @param redCapConfig The configuration of tofhir-redcap
   * */
  def startToFhirRedcap(redCapConfig: RedCapConfig): Future[Unit] = {
    try {
      // read project ids and tokens from the given config
      var projectIds: Seq[String] = Seq.empty
      var tokens: Seq[String] = Seq.empty

      redCapConfig.projects.foreach(config => {
        projectIds = projectIds :+ config.id
        tokens = tokens :+ config.token
      })

      // populate environment variables and override /redcap/.env file
      val envVarString = s"$REDCAP_PROJECT_IDS${projectIds.mkString(",")}\n$REDCAP_PROJECT_TOKENS${tokens.mkString(",")}\n$REDCAP_URL${redCapConfig.apiURL}"
      val envFile = new File(getClass.getResource("/redcap/.env").getFile)
      val fw = new FileWriter(envFile)
      try fw.write(envVarString) finally fw.close()

      // remove the slash (/) from the beginning of docker-compose.yml path which breaks the subprocess
      os.proc("docker-compose", "-f", getClass.getResource("/redcap/docker-compose.yml").getPath.substring(1), "up", "-d").call()
    } catch {
      case e: Throwable => throw new InternalServerErrorException("Error while starting tofhir-redcap", e)
    }
    Future.apply()
  }
}
