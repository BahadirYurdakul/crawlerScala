package core.utils

import java.net.URL

import core.UrlHelper
import play.api.Logger

import scala.util.{Failure, Success, Try}

case class UrlModel(parsedUrl: URL, domain: String, protocol: String, protocolWithHost: String
                    , hostWithPath: String, protocolWithHostWithPath: String)

case class ParsedUrlAndDomain(parsedUrl: URL, domain: String)

case class UrlParseException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)


object UrlModel {

  def parse(url: String, urlHelper: UrlHelper): Try[UrlModel] = {

      getParsedUrlAndDomain(url, urlHelper) match {
      case Success(value) =>
        val protocol: String = value.parsedUrl.getProtocol
        val protocolWithHost: String = protocol + "://" + value.parsedUrl.getHost
        val hostWithPath: String = "^www.".r.replaceFirstIn(value.parsedUrl.getHost, "") + value.parsedUrl.getPath +
          getQuery(value.parsedUrl)
        val protocolWithHostWithPath: String = protocolWithHost + value.parsedUrl.getPath + getQuery(value.parsedUrl)
        Success(UrlModel(value.parsedUrl, value.domain, protocol, protocolWithHost, hostWithPath, protocolWithHostWithPath))
      case Failure(fail) =>
        Logger.error(s"Url $url. Error while parsing url.")
        Failure(UrlParseException(fail.getMessage.mkString(", ")))
    }
  }

  private def getParsedUrlAndDomain(url: String ,urlHelper: UrlHelper): Try[ParsedUrlAndDomain] = {
    for {
      parsedUrl <- urlHelper.parseUrl(url)
      domain <- urlHelper.getDomainFromHost(parsedUrl.getHost)
    } yield ParsedUrlAndDomain(parsedUrl, domain)
  }

  private def getQuery(parsedUrl: URL): String = {
    if(parsedUrl.getQuery == null) "" else "?" + parsedUrl.getQuery
  }
}


