package io.tofhir.server.model

import io.tofhir.server.common.model.IToFhirResponse

/**
 *  Any exception thrown in URUK platform
 */
abstract class ToFhirError extends Exception with IToFhirResponse {
  /**
   * HTTP status code to return when this error occurs
   */
  val statusCode:Int
  /**
   * Type of the error
   */
  val `type`:String = s"https://tofhir.io/errors/${getClass.getSimpleName}"
  /**
   * Title of the error
   */
  val title:String
  /**
   * Details of the error
   */
  val detail:String

  /**
   * Inner exception
   */
  val cause:Option[Throwable] = None

  override def toString: String = {
    s"Status Code: $statusCode\n" +
      s"Type: ${`type`}\n" +
      s"Title: $title\n" +
      s"Detail: $detail\n" +
      s"Cause: ${if (cause.isDefined) cause.get.printStackTrace()}"
  }

  override def getMessage: String = {
    title + " " +detail
  }

}

case class InvalidRequest(title:String, detail:String) extends ToFhirError {
  val statusCode = 400
}

case class BadRequestData(title:String, detail:String) extends ToFhirError {
  val statusCode = 400
}

case class AlreadyExists(title:String, detail:String) extends ToFhirError {
  val statusCode = 409
}

case class OperationNotSupported(title:String, detail:String) extends ToFhirError {
  val statusCode = 422
}

case class ResourceNotFound(title:String, detail:String) extends ToFhirError {
  val statusCode = 404
}

case class InternalError(title:String, detail:String, override val cause:Option[Throwable] = None) extends ToFhirError {
  val statusCode = 500
}

///**
// * Exception thrown when a query operator is invalid or not supported
// * @param title   Title for the error
// * @param detail  Details of the error
// */
//case class InvalidQueryOperator(title:String, detail:String) extends UrukError {
//  /**
//   * HTTP status code to return when this error occurs
//   */
//  override val statusCode: Int = 400
//}
//
///**
// *  Exception thrown when access to a REST service is unauthorized
// * @param title                  Title for the error
// * @param detail                 Detail for the error
// * @param authHeaderRealmId      For WWW-Authenticate Header the realm id to use
// */
//case class UnauthorizedAccess(title:String, detail:String, authHeaderRealmId:Option[String] = None) extends UrukError {
//  override val statusCode: Int = 401
//
//  /**
//   * Prepare WWW-Authenticate header if required
//   * @return
//   */
//  def getWWWAuthenticateHeader:Option[`WWW-Authenticate`] = {
//    authHeaderRealmId
//      .map(realmId =>
//        `WWW-Authenticate`.apply(
//          HttpChallenge.apply("Bearer", realmId, Map("charset"->"UTF-8"))
//        )
//      )
//  }
//
//}
//

//
///**
// * Common serializer for UrukError objects
// */
//class UrukErrorSerializer extends CustomSerializer[UrukError](implicit format =>
//  (
//    {
//      case o:JObject =>
//        val title = (o \ "title").extract[String]
//        val detail = (o \ "detail").extract[String]
//
//        (o \ "type").extract[String]
//          .replace("http://uruk.com/platform/errors/", "")
//          .replace("http://uri.etsi.org/ngsi-ld/errors/", "") match {
//            case ir if ir == classOf[InvalidRequest].getSimpleName => InvalidRequest(title, detail)
//            case br if br == classOf[BadRequestData].getSimpleName => BadRequestData(title, detail)
//            case ae if ae == classOf[AlreadyExists].getSimpleName => AlreadyExists(title, detail)
//            case ons if ons == classOf[OperationNotSupported].getSimpleName => OperationNotSupported(title, detail)
//            case rnf if rnf == classOf[ResourceNotFound].getSimpleName => ResourceNotFound(title, detail)
//            case ie if  ie == classOf[InternalError].getSimpleName => InternalError(title, detail)
//            case ua if  ua == classOf[UnauthorizedAccess].getSimpleName => UnauthorizedAccess(title, detail)
//        }
//    },
//    {
//      case e:UrukError =>
//        JObject(Seq(
//          "type" -> JString(e.`type`),
//          "title" -> JString(e.title),
//          "detail" -> JString(e.detail)
//        ):_*)
//    }
//  )
//)
