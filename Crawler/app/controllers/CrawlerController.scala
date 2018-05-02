package controllers

import javax.inject._

import dispatchers.Contexts
import models.{PubSubMessage, RequestUrl}
import play.api.{Configuration, Logger}
import play.api.libs.json.JsValue
import play.api.mvc._
import services.CrawlerService

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}


@Singleton
class CrawlerController @Inject()(cc: ControllerComponents, crawlerService: CrawlerService, config: Configuration, contexts: Contexts)
                                 (implicit executionContext: ExecutionContext) extends AbstractController(cc) {

  def crawl(): Action[JsValue] = Action.async(parse.json) { request: Request[JsValue] =>
    Logger.debug("Crawling started.")
    PubSubMessage.getMessageFromRequest(request) flatMap { jsonData: String =>
      RequestUrl.parseRequestData(jsonData)
    } match {
      case Success(url: String) =>
        callCrawlerService(url)
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Error while parsing request data. Fail $fail")
        Future.successful(Ok(s"Error while parsing request message."))
    }
  }

  private def callCrawlerService(url: String): Future[Result] = {
    crawlerService.crawl(url) map (_ => {
      Logger.debug(s"Url: $url is crawled successfully.")
      Ok(s"Url is crawled successfully $url")
    }) recover {
      case NonFatal(fail) =>
        Logger.error(s"Url: $url cannot be crawled. Fail: $fail")
        BadRequest(s"Url: $url cannot be crawled. Fail $fail")
    }
  }
}