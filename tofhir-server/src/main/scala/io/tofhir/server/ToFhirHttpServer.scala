package io.tofhir.server

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.common.config.WebServerConfig

import java.util.concurrent.TimeUnit
import scala.concurrent.{Await, ExecutionContext, Future, Promise, blocking}
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.io.StdIn
import scala.util.{Failure, Success}

object ToFhirHttpServer extends LazyLogging {

  def start(route: Route, webServerConfig: WebServerConfig)(implicit actorSystem: ActorSystem): Unit = {
    implicit val executionContext: ExecutionContext = actorSystem.dispatcher

    Http().newServerAt(webServerConfig.serverHost, webServerConfig.serverPort).bind(route) onComplete {
      case Success(serverBinding) =>
        serverBinding.addToCoordinatedShutdown(FiniteDuration.apply(60L, TimeUnit.SECONDS))
        serverBinding.whenTerminated onComplete {
          case Success(_) =>
            actorSystem.log.info("Closing toFHIR server...")
            actorSystem.terminate()
            logger.info("toFHIR server gracefully terminated.")
          case Failure(exception) =>
            logger.error("Problem while gracefully terminating toFHIR server!", exception)
        }
        logger.info("toFHIR server online at {}", webServerConfig.serverLocation)

        //Wait for a shutdown signal
        Await.ready(waitForShutdownSignal(), Duration.Inf)
        serverBinding.terminate(FiniteDuration.apply(60L, TimeUnit.SECONDS))
      case Failure(ex) =>
        logger.error("Problem while binding to the onFhir FHIR server address and port!", ex)
        actorSystem.terminate()
    }
  }

  protected def waitForShutdownSignal()(implicit executionContext: ExecutionContext): Future[Done] = {
    val promise = Promise[Done]()
    sys.addShutdownHook {
      promise.trySuccess(Done)
    }
    Future {
      blocking {
        do {
          val line = StdIn.readLine("Write 'q' or 'quit' to stop the server...\n")
          if (line.equalsIgnoreCase("quit"))
            promise.trySuccess(Done)
        } while (!promise.isCompleted)
      }
    }
    promise.future
  }

}
