package core

import java.net.{URL, URLEncoder}
import javax.inject.{Inject, Singleton}

import core.utils.{UrlModel, UrlParseException}
import play.Logger

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


@Singleton
class UrlHelper {
  def parseUrl(url: String): Try[URL] = Try {
    val parsedUrl: URL = new URL(url)
    parsedUrl
  } match {
    case Success(parsedUrl) =>
      Success(parsedUrl)
    case Failure(NonFatal(fail)) =>
      Logger.error(s"Url: $url. Error while trying to parse url. Fail: $fail")
      Failure(fail)
  }

  def getDomainFromHost(host: String): Try[String] = {
    val domainArray: Array[String] = host.split('.')
    if(domainArray == null || domainArray.length < 2){
      Logger.error(s"Url $host is invalid while trying to get domain from host.")
      Failure(UrlParseException(s"Url $host is invalid while trying to get domain from host."))
    } else {
      Success(domainArray(domainArray.length - 2))
    }
  }

  def extractSameDomainLinks(parentParsedUrl: UrlModel, links: List[UrlModel]): List[UrlModel] = {
    links.flatMap(childUrl => {
      if (childUrl.domain == parentParsedUrl.domain && parentParsedUrl.hostWithPath != childUrl.hostWithPath)
        Some(childUrl)
      else
        None
    })
  }

  def encodeUrl(url: String): Try[String] = Try {
    URLEncoder.encode(url, "UTF-8")
  } match {
    case Success(encodedUrl) =>
      Success(encodedUrl)
    case Failure(NonFatal(fail)) =>
      Logger.error(s"Url: $url. Error while encode url. Fail: $fail")
      Failure(fail)
  }
}
