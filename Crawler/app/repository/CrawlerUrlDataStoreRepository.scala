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
class CrawlerUrlDataStoreRepository @Inject()(dataStoreClient: DataStoreClient,
                                     statusKey: StatusKey,
                                     config: Configuration)
                                    (implicit executionContext: ExecutionContext) {

  private val dataStore: Datastore = dataStoreClient.getDataStore(config.get[String]("crawlerProjectId"))
  private val keyFactory: KeyFactory = dataStoreClient.getKeyFactory(dataStore, config.get[String]("crawlerUrlKindForDataStore"))

  def getDataStore(projectId: String): Datastore = dataStoreClient.getDataStore(projectId)
  def getKeyFactory(dataStore: Datastore, kindName: String): KeyFactory = dataStore.newKeyFactory.setKind(kindName)

  def insertToDataStore(url: String, crawlerUrlModelList: List[CrawlerUrlModel]): Future[Unit] = {
    val dataList: List[Entity] = crawlerUrlModelList.map(crawlerUrlModelList => createDataStoreInstance(crawlerUrlModelList))
    dataStoreClient.insertData(dataStore, dataList) recoverWith {
      case fail: Throwable =>
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

  def getDataByUrlHostWithPath(hostWithPath: String): Future[Entity] = {
    dataStoreClient.getData(dataStore, keyFactory, hostWithPath) recoverWith {
      case NonFatal(fail) =>
        Logger.error(s"Url: $hostWithPath. Cannot get data from dataStore by its url. Fail $fail")
        return Future.failed(fail)
    }
  }

  def getDataStatusByUrlHostWithPath(hostWithPath: String): Future[Option[String]] = {
    getDataByUrlHostWithPath(hostWithPath) map { entity: Entity =>
      if(entity == null)
        None
      try {
        val stat: String = entity.getString("status")
        Option(stat)
      } catch {
        case NonFatal(fail) =>
          Logger.error(s"Url: $hostWithPath cannot get status from data store. Fail: $fail")
          None
      }
    }
  }

  def setParentStatus(url: UrlModel, statusKey: String): Future[Unit] = {
    val data: Entity = createDataStoreInstance(CrawlerUrlModel(url.hostWithPath, url.protocol, url.domain, statusKey))
    dataStoreClient.upsertData(dataStore, data) recoverWith {
      case NonFatal(fail) =>
        Logger.error(s"Url: ${url.hostWithPath}. Cannot change the status of parent. Fail $fail")
        return Future.failed(fail)
    }
  }

  def createDataStoreInstance(data: CrawlerUrlModel): Entity = {
    val key: Key = dataStoreClient.createKey(keyFactory, data.id)
    crawlerUrlEntityBuild(key,data)
  }

  def convertUrlModelToDataStoreModel(links: List[UrlModel]): List[CrawlerUrlModel] = {
    links.map(childUrl => CrawlerUrlModel(childUrl.hostWithPath, childUrl.protocol, childUrl.domain, statusKey.notCrawled))
  }
}
