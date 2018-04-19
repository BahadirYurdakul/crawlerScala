package core.utils

import java.net.URL

import core.UrlHelper

import scala.util.{Failure, Success, Try}

class UrlModel(val parsedUrl: URL, val domain: String, val protocol: String, val protocolWithHost: String
              , val hostWithPath: String, val protocolWithHostWithPath: String) {

  override def equals(o: scala.Any): Boolean = {
    if(o.getClass != this.getClass)
      return false
    hostWithPath == o.asInstanceOf[UrlModel].hostWithPath
  }

  override def hashCode(): Int = hostWithPath.hashCode
}

object UrlModel {
  def parse(url: String, urlHelper: UrlHelper): Try[UrlModel] = {
    val parsedUrl: URL = urlHelper.parseUrl(url) match {
      case Success(value) => value
      case Failure(fail) => return Failure(fail)
    }
    val domain: String = urlHelper.getDomainFromHost(parsedUrl.getHost) match {
      case Success(value) => value
      case Failure(fail) => return Failure(fail)
    }
    val protocol: String = parsedUrl.getProtocol
    val protocolWithHost: String = protocol + "://" + parsedUrl.getHost
    val hostWithPath: String = "^www.".r.replaceFirstIn(parsedUrl.getHost, "") + parsedUrl.getPath + getQuery(parsedUrl)
    val protocolWithHostWithPath: String = protocolWithHost + parsedUrl.getPath + getQuery(parsedUrl)
    Success(new UrlModel(parsedUrl, domain, protocol, protocolWithHost, hostWithPath, protocolWithHostWithPath))
  }

  def getQuery(parsedUrl: URL): String = {
    if(parsedUrl.getQuery == null) "" else "?" + parsedUrl.getQuery
  }
}
