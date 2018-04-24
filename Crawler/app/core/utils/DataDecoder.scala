package core.utils

import java.util.Base64
import javax.inject.Singleton

import play.api.Logger

import scala.util.{Failure, Try}
import scala.util.control.NonFatal

@Singleton
class DataDecoder {

  def decodeData(encodedMessage: String): Try[String] = Try {
    Base64.getDecoder.decode(encodedMessage).map(_.toChar).mkString
  } recoverWith {
    case NonFatal(fail) =>
      Logger.error(s"Error while decode string: $encodedMessage")
      Failure(fail)
  }
}
