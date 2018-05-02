package core.helpers

import com.google.inject.Singleton
import play.api.Logger

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@Singleton
class DownloadPageHelper {
  def downloadPage(url: String): Try[String] = Try {
    scala.io.Source.fromURL(url, "ISO-8859-1").mkString
  } match {
    case Success(value) =>
      Success(value)
    case Failure(NonFatal(fail)) =>
      Logger.error(s"Url $url. Error while downloading webpage.")
      Failure(fail)
  }
}
