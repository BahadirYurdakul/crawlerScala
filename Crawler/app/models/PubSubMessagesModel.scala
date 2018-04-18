package models

import play.api.libs.json._

case class PubSubData(data: String, messageId: String)
case class PubSubMessage(message: PubSubData)

object PubSubMessage {
  implicit val data: Reads[PubSubData] = Json.reads[PubSubData]
  implicit val message: Reads[PubSubMessage] = Json.reads[PubSubMessage]
}