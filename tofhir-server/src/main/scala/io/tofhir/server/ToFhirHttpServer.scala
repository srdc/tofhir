package io.tofhir.server

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.typesafe.scalalogging.LazyLogging
import io.tofhir.server.common.config.WebServerConfig

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent._
import scala.io.StdIn
import scala.util.{Failure, Success}

object ToFhirHttpServer extends LazyLogging {

  def start(route: Route, webServerConfig: WebServerConfig)(implicit actorSystem: ActorSystem): Unit = {
    implicit val executionContext: ExecutionContext = actorSystem.dispatcher

    val serverBindingFuture = Http().newServerAt(webServerConfig.serverHost, webServerConfig.serverPort).bind(route)
      .map(serverBinding => {
        serverBinding.addToCoordinatedShutdown(hardTerminationDeadline = FiniteDuration(10, TimeUnit.SECONDS))
        serverBinding.whenTerminated onComplete {
          case Success(_) =>
            logger.info("Closing toFHIR HTTP server...")
            actorSystem.terminate()
            logger.info("toFHIR HTTP server gracefully terminated.")
          case Failure(exception) =>
            logger.error("Problem while gracefully terminating toFHIR HTTP server!", exception)
        }
        serverBinding
      })

    var serverBinding: Option[Http.ServerBinding] = None
    try {
      serverBinding = Some(Await.result(serverBindingFuture, FiniteDuration(10L, TimeUnit.SECONDS)))
      logger.info(s"toFHIR server ready at ${webServerConfig.serverHost}:${webServerConfig.serverPort}")
    } catch {
      case e: Exception =>
        logger.error("Problem while binding to the given HTTP address and port!", e)
        actorSystem.terminate()
    }

    //Wait for a shutdown signal
    Await.ready(waitForShutdownSignal(), Duration.Inf)
    serverBinding.get.terminate(FiniteDuration.apply(10L, TimeUnit.SECONDS))
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
