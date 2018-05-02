package core.gcloud

import javax.inject.{Inject, Singleton}

import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.{ProjectTopicName, PubsubMessage, TopicName}
import play.api.{ConfigLoader, Configuration, Logger}

import scala.concurrent.{ExecutionContext, Future, Promise}
import com.google.api.core.{ApiFuture, ApiFutures}
import java.util

import com.typesafe.config.{ConfigFactory, ConfigList, ConfigValue}
import dispatchers.Contexts
import models.GoogleProjectModel
import play.api.inject.ApplicationLifecycle
import play.api.libs.json.{JsValue, Json}

import scala.collection.mutable
import scala.util.Try
import scala.util.control.NonFatal

@Singleton
class PubSubClient @Inject()(applicationLifecycle: ApplicationLifecycle, config: Configuration, contexts: Contexts) {

  private val publishers: mutable.Map[Int, Publisher] = mutable.Map[Int, Publisher]()

  private val ps = config.getConfigList("googleProjects") map  { confs: java.util.List[Configuration] =>
    confs.toArray() map { conf: AnyRef =>
      val project = conf.asInstanceOf[Configuration]
      getPublisher(project.get[String]("id"), project.get[String]("pubSubTopicName"))
    }
  }


  //todo solve deprecated
  /*
  private val ps = defaultConfig.getConfigList("googleProjects").toArray().asInstanceOf[Array[Config]]
    .map { project: Config =>
      getPublisher(project.getString("id"), project.getString("pubSubTopicName"))
    }
    */


  /*
  private val ps = config.underlying.getConfigList("googleProjects").toArray().asInstanceOf[Array[Config]]
    .map { project: Config =>
      getPublisher(project.getString("id"), project.getString("pubSubTopicName"))
    }
    */


  /*
  private val ps = config.getConfigList("googleProjects") map  { confs: java.util.List[Configuration] =>
    confs.toArray() map { conf: AnyRef =>
      val project = conf.asInstanceOf[Configuration]
      getPublisher(project.get[String]("id"), project.get[String]("pubSubTopicName"))
    }
  }
  */


  /*
  private val ps = config.underlying.getConfigList("googleProjects").toArray().asInstanceOf[Array[Configuration]]
  .map { project: Configuration =>
    getPublisher(project.get[String]("id"), project.get[String]("pubSubTopicName"))
  }

 */


  /*

    private val ps: List[Publisher] = {
    val a = config.get("googleProjects") map { project =>
      getPublisher(project.id, project.pubSubTopicName)
    }
  }
    config.getConfigList("googleProjects") foreach { confs: java.util.List[Configuration] =>
    confs forEach { conf: Configuration =>
      getPublisher(conf.get[String]("id"), conf.get[String]("pubSubTopicName"))
      //Logger.error(s"ajsvakvbakjb::  ${conf.get[String]("id")}")
    }
  }

    val a = config.getList("googleProjects")
  Logger.error(s"err: $a")

  private val googleProjects: Seq[String] = config.get[Seq[String]]("googleProjects")
  Logger.error("pb :::" + googleProjects.toString())
  */

  applicationLifecycle.addStopHook(() => {
    Future.successful(publishers.foreach(publisher => shutDownPublisher(publisher._2)))
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
      case NonFatal(_) =>
        Logger.error(s"Error while shut down publisher. Publisher $publisher")
    }
  }
}
