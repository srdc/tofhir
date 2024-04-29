package io.tofhir.engine.logback

import ch.qos.logback.classic.spi.{ILoggingEvent, IThrowableProxy, LoggerContextVO}
import ch.qos.logback.more.appenders.marker.MapMarker
import net.logstash.logback.encoder.LogstashEncoder
import net.logstash.logback.marker.LogstashMarker
import net.logstash.logback.marker.Markers.appendEntries

import java.util

/**
 * An encoder for converting MapMarker objects to LogstashMarker objects.
 */
class MapMarkerToLogstashMarkerEncoder extends LogstashEncoder {

  /**
   * Encodes the logging event by converting MapMarker to LogstashMarker and then calling super.encode().
   *
   * @param event The logging event to encode.
   * @return The encoded logging event as a byte array.
   */
  override def encode(event: ILoggingEvent): Array[Byte] = {
    val modifiedEvent = modifyEventWithLogstashMarker(event)
    super.encode(modifiedEvent)
  }

  /**
   * Modifies the logging event with a LogstashMarker if it contains a MapMarker.
   *
   * @param event The logging event to modify.
   * @return The modified logging event.
   */
  private def modifyEventWithLogstashMarker(event: ILoggingEvent): ILoggingEvent = {
    val map = getMapMarkerData(event.getMarker)
    if (map.isDefined) {
      val logstashMarker = toLogstashMarker(map.get)
      new ModifiedLoggingEvent(event, logstashMarker)
    } else {
      event
    }
  }

  /**
   * Extracts the data from a MapMarker.
   *
   * @param marker The marker to extract data from.
   * @return An Optional containing the marker data if the marker is a MapMarker, otherwise empty.
   */
  private def getMapMarkerData(marker: org.slf4j.Marker): Option[util.Map[String, _]] = {
    marker match {
      case m: MapMarker => Some(m.getMap)
      case _ => None
    }
  }

  /**
   * Converts a Map of marker data to a LogstashMarker.
   *
   * @param map The map of marker data.
   * @return The corresponding LogstashMarker.
   */
  private def toLogstashMarker(map: util.Map[String, _]): LogstashMarker = {
    appendEntries(map)
  }

  /**
   * A wrapper class for the logging event that delegates to the original event but with a different marker.
   */
  private class ModifiedLoggingEvent(originalEvent: ILoggingEvent, marker: LogstashMarker) extends ILoggingEvent {
    override def getMarker: org.slf4j.Marker = marker

    // Implement other methods by delegating to the original event
    override def getTimeStamp: Long = originalEvent.getTimeStamp

    override def getLevel: ch.qos.logback.classic.Level = originalEvent.getLevel

    override def getThreadName: String = originalEvent.getThreadName

    override def getMessage: String = originalEvent.getMessage

    override def getArgumentArray: Array[AnyRef] = originalEvent.getArgumentArray

    override def getFormattedMessage: String = originalEvent.getFormattedMessage

    override def getLoggerName: String = originalEvent.getLoggerName

    override def getLoggerContextVO: LoggerContextVO = originalEvent.getLoggerContextVO

    override def getThrowableProxy: IThrowableProxy = originalEvent.getThrowableProxy

    override def getCallerData: Array[StackTraceElement] = originalEvent.getCallerData

    override def hasCallerData: Boolean = originalEvent.hasCallerData

    override def getMDCPropertyMap: util.Map[String, String] = originalEvent.getMDCPropertyMap

    override def getMdc: util.Map[String, String] = originalEvent.getMdc

    override def prepareForDeferredProcessing(): Unit = originalEvent.prepareForDeferredProcessing()
  }
}
