package core

import java.net.{URL, URLEncoder}
import javax.inject.{Inject, Singleton}

import play.Logger

import scala.util.{Failure, Try}

@Singleton
class UrlHelper @Inject()() {
  def parseUrl(url: String): Try[URL] = Try {
    val parsedUrl: URL = new URL(url)
    parsedUrl
  }

  def getDomainFromHost(host: String): Try[String] = Try {
    val domainArray: Array[String] = host.split('.')
    if(domainArray == null || domainArray.length < 2){
      Logger.error(s"Url host is invalid while trying to get domain from host. Host: $host")
      return Failure(new Exception(s"Url host is invalid. Host: $host"))
    } else {
      domainArray(domainArray.length - 2)
    }
  }

  def encodeUrl(url: String): Try[String] = Try {
    URLEncoder.encode(url, "UTF-8")
  }
}
