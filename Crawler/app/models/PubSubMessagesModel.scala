package models

import java.util.Base64

import play.api.Logger
import play.api.libs.json._
import play.api.mvc.Request

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class InvalidJsonBodyException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class PubSubMessageParseException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class PubSubData(data: String, messageId: String)
case class PubSubMessage(message: PubSubData)

object PubSubMessage {
  implicit val data: Reads[PubSubData] = Json.reads[PubSubData]
  implicit val message: Reads[PubSubMessage] = Json.reads[PubSubMessage]

  def getMessageFromRequest(request: Request[JsValue]): Try[String] = {
    parseRequestMessage(request) match {
      case Success(encodedPubSubData: String) =>
        decodeData(encodedPubSubData) match {
          case Success(decodedData: String) => Success(decodedData)
          case Failure(NonFatal(fail)) => Failure(fail)
        }
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Error while parse pubSub message. ErrMessage: $fail")
        Failure(fail)
    }
  }

  private def parseRequestMessage(request: Request[JsValue]): Try[String] = {
    request.body.validate(PubSubMessage.message) match  {
      case message: JsSuccess[PubSubMessage] =>
        Success(message.get.message.data)
      case fail: JsError =>
        Logger.error(s"Invalid json body. Fail $fail")
        Failure(InvalidJsonBodyException("Invalid json body. Fail $fail"))
    }
  }

  def decodeData(encodedMessage: String): Try[String] = Try {
    Base64.getDecoder.decode(encodedMessage).map(_.toChar).mkString
  } recoverWith {
    case NonFatal(fail) =>
      Logger.error(s"Error while decode string: $encodedMessage")
      Failure(fail)
  }
}