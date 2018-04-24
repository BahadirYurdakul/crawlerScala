package models

import play.api.Logger
import play.api.libs.json._

import scala.util.{Failure, Try}


case class RequestUrl private(name: String)

object RequestUrl {
  val url: Reads[RequestUrl] = Json.reads[RequestUrl]
  def jsonBuildWithUrl(message: String): JsValue = Json.obj("name" -> s"$message")

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
}

