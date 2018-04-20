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

@Singleton
class CrawlerController @Inject()(cc: ControllerComponents, crawlerService: CrawlerService, config: Configuration)
                                 (implicit executor: ExecutionContext) extends AbstractController(cc) {

  val maxTryCountForUrl: Integer = config.getOptional[String]("maxTryCountForUrl").getOrElse("10").toInt
  val tryCountsOfMessage: mutable.Map[String, Integer] = mutable.Map()

  def crawl(): Action[JsValue] = Action.async(parse.json) { request: Request[JsValue] =>
    Logger.debug("Crawling started.")
    val decodedPubSubData: Either[String, String] = decodeRequestData(request)
    val eitherUrl: Either[String, String] = extractUrlFromDecodedPubSubData(decodedPubSubData)

    val result: Future[Result] = eitherUrl match {
      case Right(url: String) =>
        callCrawlerService(url)
      case Left(errMessage: String) =>
        Logger.error(s"Cannot extract url from pubSubMessage. Err: $errMessage")
        Future.successful(Ok("Cannot extract url from pubSubMessage."))
    }
    result
  }

  def callCrawlerService(url: String): Future[Result] = {
    incrementTryCount(url) match {
      case Right(_) =>
        crawlerService.crawl(url) map (_ => {
          Logger.debug(s"Url: $url is crawled successfully.")
          deleteFromTryCount(url)
          Ok(s"Url is crawled successfully $url")
        }) recover {
          case NonFatal(fail) =>
            Logger.error(s"Url: $url cannot be crawled. Fail: $fail")
            BadRequest(s"Url: $url cannot be crawled. Fail $fail")
        }
      case Left(errorMessage: String) =>
        Logger.error(errorMessage)
        Future.successful(Ok(errorMessage))
    }
  }

  def decodeRequestData(request: Request[JsValue]): Either[String, String] = {
    parseRequestMessage(request) match {
      case Right(encodedPubSubData: String) =>
        decodeData(encodedPubSubData) match {
          case Success(decodedData: String) => Right(decodedData)
          case Failure(NonFatal(fail)) => Left(s"Error while decode pubSubData. Fail: $fail")
        }
      case Left(errorMessage: String) =>
        Logger.error(s"Error while parse pubSub message. ErrMessage: $errorMessage")
        Left(s"Error while parse pubSub message. ErrMessage: $errorMessage")
    }
  }

  def extractUrlFromDecodedPubSubData(decodedPubSubData: Either[String, String]): Either[String, String] = {
    decodedPubSubData match {
      case Right(data: String) =>
        parseRequestData(data)  match {
          case Success(url: String) => Right(url)
          case Failure(NonFatal(fail)) => Left(s"Error while parse pubSub data and extract url. Fail $fail")
        }
      case Left(errMessage: String) =>
        Logger.error(s"Error while decoded pubSubData. Err: $errMessage")
        Left(s"Error while decoded pubSubData. Err: $errMessage")
    }
  }

  def parseRequestMessage(request: Request[JsValue]): Either[String, String] = {
    request.body.validate(PubSubMessage.message) match  {
      case message: JsSuccess[PubSubMessage] =>
        Right(message.get.message.data)
      case fail: JsError =>
        Logger.error(s"Invalid json body. Fail $fail")
        Left(s"Invalid json body. Fail $fail")
    }
  }

  def decodeData(encodedMessage: String): Try[String] = Try {
    Base64.getDecoder.decode(encodedMessage).map(_.toChar).mkString
  }

  def parseRequestData(data: String): Try[String] = Try {
    val jsonData: JsValue = Json.parse(s"""$data""")
    jsonData.validate(RequestUrl.url) match {
      case url: JsSuccess[RequestUrl] =>
        Logger.debug(s"url is ${url.get.name}")
        url.get.name
      case fail: JsError =>
        Logger.error(s"Invalid json body. Fail $fail")
        return Failure(new Exception(fail.errors.mkString(", ")))
    }
  }

  def incrementTryCount(url: String): Either[String, Int] = {
    tryCountsOfMessage.get(url) match {
      case Some(value: Integer) =>
        tryCountsOfMessage += url -> (value + 1)
        if (value >= maxTryCountForUrl) {
          Logger.error(s"Url: $url. Try count exceeded $value")
          Left(s"Url: $url. Try count exceeded $value")
        } else
          Right(value + 1)
      case None =>
        tryCountsOfMessage += url -> 1
        Right(1)
    }
  }

  def deleteFromTryCount(messageId: String): Unit = {
    tryCountsOfMessage.remove(messageId)
  }
}