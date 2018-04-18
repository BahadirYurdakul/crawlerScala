package models

import play.api.libs.json._


case class RequestUrl private(name: String)

object RequestUrl {
  val url: Reads[RequestUrl] = Json.reads[RequestUrl]
  def jsonBuildWithUrl(message: String): JsValue = Json.obj("name" -> s"$message")
}

