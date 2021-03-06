package core.helpers

import java.net.MalformedURLException
import javax.inject.{Inject, Singleton}

import core.UrlHelper
import core.utils.UrlModel
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import play.api.Logger

import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@Singleton
class ScrapeLinksHelper @Inject ()(urlHelper: UrlHelper) {

  def scrapeLinks(baseUrl: String,rawHtml: String): Try[List[UrlModel]] = Try {
    val parsedUrls: mutable.Set[UrlModel] = mutable.Set()
    val doc: Document = Jsoup.parse(rawHtml, baseUrl)
    val linksOnPage = doc.body.select("[href]")

    linksOnPage.forEach(linkElement => {
      try {
        val link: String = linkElement.attr("abs:href")
        Logger.debug(s"Url: $baseUrl. Extracted link $link")
        val childUrl: UrlModel = UrlModel.parse(link, urlHelper) match {
          case Success(value: UrlModel) => value
          case Failure(NonFatal(fail)) =>
            Logger.error(s"Url: $baseUrl. Error while extracting child link model. Child link: $link. Fail $fail")
            throw fail
        }
        parsedUrls += childUrl
      } catch {
        case e: MalformedURLException => Logger.error(s"Url: $baseUrl. Malformed url exception in child link. Exception: $e")
        case NonFatal(exc) => Logger.info(s"Url: $baseUrl. Exception while parsing child link. Exception: $exc")
      }
    })
    parsedUrls.toList
  }
}
