package core.utils

import java.net.URL

import core.UrlHelper
import play.api.Logger

import scala.util.{Failure, Success, Try}

case class UrlModel(parsedUrl: URL, domain: String, protocol: String, protocolWithHost: String
                    , hostWithPath: String, protocolWithHostWithPath: String)


object UrlModel {

  def parse(url: String, urlHelper: UrlHelper): Try[UrlModel] = {

    val parsedUrl: URL = urlHelper.parseUrl(url) match {
      case Success(value) => value
      case Failure(fail) =>
        Logger.error(s"Url $url. Error while parse: ${fail.getMessage}")
        throw UrlParseException(s"Url $url. Error while parse: ${fail.getMessage}")
    }

    val domain: String = urlHelper.getDomainFromHost(parsedUrl.getHost) match {
      case Success(value) => value
      case Failure(fail) =>
        Logger.error(s"Url $url. Error while get domain: ${fail.getMessage}")
        throw UrlParseException(s"Url $url. Error while get domain: ${fail.getMessage}")
    }

    val protocol: String = parsedUrl.getProtocol
    val protocolWithHost: String = protocol + "://" + parsedUrl.getHost
    val hostWithPath: String = "^www.".r.replaceFirstIn(parsedUrl.getHost, "") + parsedUrl.getPath + getQuery(parsedUrl)
    val protocolWithHostWithPath: String = protocolWithHost + parsedUrl.getPath + getQuery(parsedUrl)
    Success(UrlModel(parsedUrl, domain, protocol, protocolWithHost, hostWithPath, protocolWithHostWithPath))

  }

  def getQuery(parsedUrl: URL): String = {
    if(parsedUrl.getQuery == null) "" else "?" + parsedUrl.getQuery
  }
}

case class UrlParseException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)


