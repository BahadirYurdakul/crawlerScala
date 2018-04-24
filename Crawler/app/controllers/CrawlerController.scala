package controllers

import java.util.Base64
import javax.inject._

import core.utils.DataDecoder
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
class CrawlerController @Inject()(cc: ControllerComponents, crawlerService: CrawlerService, config: Configuration,
                                  dataDecoder: DataDecoder)
                                 (implicit executor: ExecutionContext) extends AbstractController(cc) {

  val maxTryCountForUrl: Integer = config.getOptional[String]("maxTryCountForUrl").getOrElse("10").toInt
  val tryCountsOfMessage: mutable.Map[String, Integer] = mutable.Map()

  def crawl(): Action[JsValue] = Action.async(parse.json) { request: Request[JsValue] =>
    Logger.debug("Crawling started.")
    val decodedPubSubData: Either[Throwable, String] = decodeRequestData(request)
    val eitherUrl: Either[Throwable, String] = extractUrlFromDecodedPubSubData(decodedPubSubData)

    val result: Future[Result] = eitherUrl match {
      case Right(url: String) =>
        callCrawlerService(url)
      case Left(fail: Throwable) =>
        Logger.error(s"Cannot extract url from pubSubMessage. Err: $fail")
        Future.successful(Ok("Cannot extract url from pubSubMessage."))
    }
    result
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

  def decodeRequestData(request: Request[JsValue]): Either[Throwable, String] = {
    parseRequestMessage(request) match {
      case Right(encodedPubSubData: String) =>
        decodeData(encodedPubSubData) match {
          case Success(decodedData: String) => Right(decodedData)
          case Failure(NonFatal(fail)) => Left(fail)
        }
      case Left(fail: Throwable) =>
        Logger.error(s"Error while parse pubSub message. ErrMessage: $fail")
        Left(fail)
    }
  }

  def extractUrlFromDecodedPubSubData(decodedPubSubData: Either[Throwable, String]): Either[Throwable, String] = {
    decodedPubSubData match {
      case Right(data: String) =>
        parseRequestData(data) match {
          case Success(url: String) => Right(url)
          case Failure(NonFatal(fail)) => Left(PubSubMessageParseException(s"Error while parse pubSub data and extract url. Fail $fail"))
        }
      case Left(fail: Throwable) =>
        Logger.error(s"Error while decode pubSubData. Err: $fail")
        Left(fail)
    }
  }

  def parseRequestMessage(request: Request[JsValue]): Either[Throwable, String] = {
    request.body.validate(PubSubMessage.message) match  {
      case message: JsSuccess[PubSubMessage] =>
        Right(message.get.message.data)
      case fail: JsError =>
        Logger.error(s"Invalid json body. Fail $fail")
        Left(InvalidJsonBodyException("Invalid json body. Fail $fail"))
    }
  }

  def parseRequestData(data: String): Try[String] = Try {
    val jsonData: JsValue = Json.parse(s"""$data""")
    jsonData.validate(RequestUrl.url) match {
      case url: JsSuccess[RequestUrl] =>
        Logger.debug(s"url is ${url.get.name}")
        url.get.name
      case fail: JsError =>
        Logger.error(s"Invalid json body. Fail $fail")
        throw new Exception(fail.errors.mkString(", "))
    }
  }
}