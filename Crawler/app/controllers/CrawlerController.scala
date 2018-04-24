package controllers

import java.util.Base64
import javax.inject._

import models.{PubSubMessage, RequestUrl}
import play.api.{Configuration, Logger}
import play.api.libs.json.{JsError, JsSuccess, JsValue, Json}
import play.api.mvc._
import services.CrawlerService

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class DecodeDataException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class PubSubMessageParseException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class InvalidJsonBodyException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class TryCountExceededException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)


@Singleton
class CrawlerController @Inject()(cc: ControllerComponents, crawlerService: CrawlerService, config: Configuration)
                                 (implicit executor: ExecutionContext) extends AbstractController(cc) {

  val maxTryCountForUrl: Integer = config.getOptional[String]("maxTryCountForUrl").getOrElse("10").toInt
  val tryCountsOfMessage: mutable.Map[String, Integer] = mutable.Map()

  def crawl(): Action[JsValue] = Action.async(parse.json) { request: Request[JsValue] =>
    Logger.debug("Crawling started.")
    PubSubMessage.getMessageFromRequest(request) flatMap { jsonData: String =>
      RequestUrl.parseRequestData(jsonData)
    } match {
      case Success(url: String) => callCrawlerService(url)
      case Failure(fail) =>
        Logger.error(s"Error while parsing request message. Fail $fail")
        Future.successful(Ok(s"Error while parsing request message."))
    }
  }

  def callCrawlerService(url: String): Future[Result] = {
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