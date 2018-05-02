package core.gcloud

import javax.inject.{Inject, Singleton}

import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{ProjectTopicName, PubsubMessage}
import play.api.{Configuration, Logger}

import scala.concurrent.{ExecutionContext, Future}
import com.google.api.core.{ApiFuture, ApiFutures}
import java.util

import com.typesafe.config.Config
import dispatchers.Contexts
import play.api.inject.ApplicationLifecycle
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

case class PubSubNotFoundException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

@Singleton
class PubSubClient @Inject()(applicationLifecycle: ApplicationLifecycle, config: Configuration, contexts: Contexts) {

  private val confArr: Array[AnyRef] = config.underlying.getConfigList("googleProjects").toArray
  private val publisherArr: Array[Publisher] = confArr map { conf: AnyRef =>
    val project = conf.asInstanceOf[Config]
    createPublisher(project.getString("id"), project.getString("pubSubTopicName"))
  }

  applicationLifecycle.addStopHook(() => {
    Future.successful(publisherArr.foreach(_ => shutDownPublisher(_)))
  })

  private def topicNameBuilder(projectName: String, topicName: String): ProjectTopicName = {
    ProjectTopicName.of(projectName, topicName)
  }

  private def publisherBuilder(topic: ProjectTopicName): Publisher = {
    Publisher.newBuilder(topic).build
  }

  def publishToPubSub(projectId: String, topicId: String, messages: List[JsValue]): Future[Unit] = {
    implicit val executor: ExecutionContext = contexts.gcloudOperations
    Future {
      getPublisher(projectId, topicId) match {
        case None =>
          Logger.error(s"ProjectId: $projectId, topicId: $topicId. Google Project Not Found.")
          throw PubSubNotFoundException(s"ProjectId: $projectId, topicId: $topicId. Google Project Not Found.")
        case Some(publisher) =>
          val messageIdFutures: util.ArrayList[ApiFuture[String]] = new util.ArrayList[ApiFuture[String]]
          for (message: JsValue <- messages) {
            val pubSubMessage: PubsubMessage = buildPubSubMessage(message)
            val messageIdFuture: ApiFuture[String] = publisher.publish(pubSubMessage)
            messageIdFutures.add(messageIdFuture)
          }
          val messageIds: util.List[String] = ApiFutures.allAsList(messageIdFutures).get
          messageIds.forEach(messageId => {Logger.debug(s"Message: published with message ID: $messageId by Publisher $publisher")})
      }
    }
  }

  private def getPublisher(projectId: String, topicId: String): Option[Publisher] = {
    val topic: ProjectTopicName = topicNameBuilder(projectId, topicId)
    val publishers = publisherArr.filter(ps => topic == ps.getTopicName)
    if(publishers == null || publishers.length == 0)
      None
    else
      Some(publishers(0))
  }


  private def createPublisher(projectId: String, topicId: String): Publisher = {
    val topic: ProjectTopicName = topicNameBuilder(projectId, topicId)
    publisherBuilder(topic)
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
      case NonFatal(_) =>
        Logger.error(s"Error while shut down publisher. Publisher $publisher")
    }
  }
}
