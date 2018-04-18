package core.gcloud

import javax.inject.{Inject, Singleton}

import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{ProjectTopicName, PubsubMessage, TopicName}
import play.api.Logger

import scala.concurrent.{ExecutionContext, Future, Promise}
import com.google.api.core.{ApiFuture, ApiFutures}
import java.util

import play.api.inject.ApplicationLifecycle
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable
import scala.util.Try

@Singleton
class PubSubClient @Inject()(applicationLifecycle: ApplicationLifecycle)(implicit executionContext: ExecutionContext) {

  private val publishers: mutable.Map[Int, Publisher] = mutable.Map[Int, Publisher]()

  applicationLifecycle.addStopHook(() => {
    Future.successful(publishers.foreach(publisher => shutDownPublisher(publisher._2)))
  })

  private def topicNameBuilder(projectName: String, topicName: String): ProjectTopicName = {
    ProjectTopicName.of(projectName, topicName)
  }

  private def publisherBuilder(topic: ProjectTopicName): Publisher = {
    Publisher.newBuilder(topic).build
  }

  def publishToPubSub(projectId: String, topicId: String, messages: List[JsValue]): Future[Unit] = Future {
    val publisher: Publisher = getPublisher(projectId, topicId)
    val messageIdFutures: util.ArrayList[ApiFuture[String]] = new util.ArrayList[ApiFuture[String]]
    for (message: JsValue <- messages) {
      val pubSubMessage: PubsubMessage = buildPubSubMessage(message)
      val messageIdFuture: ApiFuture[String] = publisher.publish(pubSubMessage)
      messageIdFutures.add(messageIdFuture)
    }
    val messageIds: util.List[String] = ApiFutures.allAsList(messageIdFutures).get
    messageIds.forEach(messageId => {
      Logger.debug(s"Message: published with message ID: $messageId by Publisher $publisher")
    })
  }

  private def getPublisher(projectId: String, topicId: String): Publisher = {
    val uniqueHash: Int = projectId.hashCode + topicId.hashCode
    publishers.get(uniqueHash) match {
      case Some(publisher: Publisher) => publisher
      case None =>
        val topic: ProjectTopicName = topicNameBuilder(projectId, topicId)
        val publisher: Publisher = publisherBuilder(topic)
        publishers += uniqueHash -> publisher
        publisher
    }
  }

  private def buildPubSubMessage(message: JsValue): PubsubMessage = {
    val data: ByteString = ByteString.copyFromUtf8(Json.stringify(message))
    PubsubMessage.newBuilder.setData(data).build
  }

  private def shutDownPublisher(publisher: Publisher) {
    Logger.debug(s"Publisher shut down. Publisher $publisher")
    try {
      publisher.shutdown()
    } catch {
      case _: IllegalStateException =>
        Logger.debug(s"Publisher already shut down. Publisher $publisher")
      case _: Throwable =>
        Logger.error(s"Error while shut down publisher. Publisher $publisher")
    }
  }
}
