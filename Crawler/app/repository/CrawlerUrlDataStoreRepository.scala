package repository

import javax.inject.{Inject, Singleton}

import com.google.cloud.datastore.{Datastore, Entity, Key, KeyFactory}
import core.gcloud.DataStoreClient
import core.utils.{StatusKey, UrlModel}
import play.Logger
import play.api.Configuration
import com.google.cloud.Timestamp

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

case class CrawlerUrlModel(id: String, protocol: String, domain: String, status: String)

@Singleton
class CrawlerUrlDataStoreRepository @Inject()(dataStoreClient: DataStoreClient, config: Configuration)
                                             (implicit executionContext: ExecutionContext) {

  private val dataStore: Datastore = dataStoreClient.getDataStore(config.get[String]("crawlerProjectId"))
  private val keyFactory: KeyFactory = dataStoreClient.getKeyFactory(dataStore, config.get[String]("crawlerUrlKindForDataStore"))

  def insertToDataStore(url: String, crawlerUrlModelList: List[CrawlerUrlModel]): Future[Unit] = {
    val dataList: List[Entity] = crawlerUrlModelList.map(crawlerUrlModelList => createDataStoreInstance(crawlerUrlModelList))
    dataStoreClient.insertData(dataStore, dataList) recoverWith {
      case NonFatal(fail) =>
        Logger.error(s"Url: $url. Cannot insert dataList to dataStore. Data $crawlerUrlModelList Fail $fail")
        throw fail
    }
  }

  def crawlerUrlEntityBuild(key: com.google.cloud.datastore.Key, dataStoreModel: CrawlerUrlModel): Entity = {
    val task: Entity = Entity.newBuilder(key)
      .set("protocol", dataStoreModel.protocol)
      .set("domain", dataStoreModel.domain)
      .set("status", dataStoreModel.status)
      .set("access_time", Timestamp.now())
      .build()
    task
  }

  def getDataByUrlHostWithPath(hostWithPath: String): Future[Option[Entity]] = {
    dataStoreClient.getData(dataStore, keyFactory, hostWithPath) recoverWith {
      case NonFatal(fail) =>
        Logger.error(s"Url: $hostWithPath. Cannot get data from dataStore by its url. Fail $fail")
        Future.failed(fail)
    }
  }

  def getDataStatusByUrlHostWithPath(hostWithPath: String): Future[Option[StatusKey.Value]] = {
    getDataByUrlHostWithPath(hostWithPath) map { entity: Option[Entity] =>
      entity match {
        case Some(value: Entity) =>
          try {
            Some(StatusKey.values.find(_.toString == value.getString("status")).getOrElse(StatusKey.Unknown))
          } catch {
            case NonFatal(fail) =>
              Logger.error(s"Url: $hostWithPath cannot get status from data store. Fail: $fail")
              None
          }
        case None => None
      }
    }
  }

  def setParentStatus(url: UrlModel, status: StatusKey.Value): Future[Unit] = {
    val data: Entity = createDataStoreInstance(CrawlerUrlModel(url.hostWithPath, url.protocol, url.domain, status.toString))
    dataStoreClient.upsertData(dataStore, data) recoverWith {
      case NonFatal(fail) =>
        Logger.error(s"Url: ${url.hostWithPath}. Cannot change the status of parent. Fail $fail")
        return Future.failed(fail)
    }
  }

  def createDataStoreInstance(data: CrawlerUrlModel): Entity = {
    val key: Key = dataStoreClient.createKey(keyFactory, data.id)
    crawlerUrlEntityBuild(key, data)
  }

  def convertUrlModelToDataStoreModel(links: List[UrlModel]): List[CrawlerUrlModel] = {
    links.map(childUrl => CrawlerUrlModel(childUrl.hostWithPath, childUrl.protocol, childUrl.domain, StatusKey.NotCrawled.toString))
  }
}
