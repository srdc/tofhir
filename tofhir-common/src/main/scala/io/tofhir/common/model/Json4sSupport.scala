package io.tofhir.common.model

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes.`application/json`
import akka.http.scaladsl.unmarshalling.{FromEntityUnmarshaller, Unmarshaller}
import akka.util.ByteString
import org.json4s.{Formats, MappingException, Serialization, jackson}

import java.lang.reflect.InvocationTargetException

/**
 * Automatic to and from JSON marshalling/unmarshalling using an in-scope *Json4s* protocol.
 *
 * Pretty printing is enabled if an implicit [[Json4sSupport.ShouldWritePretty.True]] is in scope.
 */
object Json4sSupport extends Json4sSupport {

  implicit lazy val formats: Formats = JsonFormats.getFormats
  implicit lazy val serialization: Serialization = jackson.Serialization

  sealed abstract class ShouldWritePretty

  object ShouldWritePretty {
    object True extends ShouldWritePretty

    object False extends ShouldWritePretty
  }

}

/**
 * Automatic to and from JSON marshalling/unmarshalling using an in-scope *Json4s* protocol.
 *
 * Pretty printing is enabled if an implicit [[Json4sSupport.ShouldWritePretty.True]] is in scope.
 */
trait Json4sSupport {

  import Json4sSupport._

  private val jsonStringUnmarshaller =
    Unmarshaller.byteStringUnmarshaller
      .forContentTypes(`application/json`)
      .mapWithCharset {
        case (ByteString.empty, _) => throw Unmarshaller.NoContentException
        case (data, charset) => data.decodeString(charset.nioCharset.name)
      }

  private val jsonStringMarshaller = Marshaller.stringMarshaller(`application/json`)

  /**
   * HTTP entity => `A`
   *
   * @tparam A type to decode
   * @return unmarshaller for `A`
   */
  implicit def unmarshaller[A: Manifest](implicit serialization: Serialization, formats: Formats): FromEntityUnmarshaller[A] =
    jsonStringUnmarshaller
      .map(s => serialization.read(s))
      .recover { _ =>
        _ => {
          case MappingException(_, ite: InvocationTargetException) => throw ite.getCause
        }
      }

  /**
   * `A` => HTTP entity
   *
   * @tparam A type to encode, must be upper bounded by `AnyRef`
   * @return marshaller for any `A` value
   */
  implicit def marshaller[A <: AnyRef](implicit serialization: Serialization, formats: Formats,
                                       shouldWritePretty: ShouldWritePretty = ShouldWritePretty.False): ToEntityMarshaller[A] =
    shouldWritePretty match {
      case ShouldWritePretty.False =>
        jsonStringMarshaller.compose(serialization.write[A])
      case ShouldWritePretty.True =>
        jsonStringMarshaller.compose(serialization.writePretty[A])
    }
}
