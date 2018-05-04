package repository

import javax.inject.{Inject, Singleton}

import com.google.cloud.datastore.{Datastore, Entity, Key, KeyFactory}
import core.gcloud.DataStoreClient
import core.utils.{StatusKey, UrlModel}
import play.Logger
import play.api.Configuration
import com.google.cloud.Timestamp
import dispatchers.ExecutionContexts

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

case class CrawlerUrlDataStoreModel(id: String, protocol: String, domain: String, status: StatusKey.Value, tryCount: Int)

@Singleton
class CrawlerUrlDataStoreRepository @Inject()(dataStoreClient: DataStoreClient, config: Configuration, ExecutionContexts: ExecutionContexts) {

  private val dataStore: Datastore = dataStoreClient.getDataStore(config.get[String]("crawlerProjectId"))
  private val keyFactory: KeyFactory = dataStoreClient.getKeyFactory(dataStore, config.get[String]("crawlerUrlKindForDataStore"))

  def insertToDataStore(url: String, CrawlerUrlDataStoreModelList: List[CrawlerUrlDataStoreModel]): Future[Unit] = {
    implicit val executor: ExecutionContext = ExecutionContexts.dbWriteOperations
    val dataList: List[Entity] = CrawlerUrlDataStoreModelList.map(CrawlerUrlDataStoreModelList => createDataStoreInstance(CrawlerUrlDataStoreModelList))
    dataStoreClient.insertData(dataStore, dataList) recover {
      case NonFatal(fail) =>
        Logger.error(s"Url: $url. Error while inserting data to dataStore. Data: $CrawlerUrlDataStoreModelList. Fail: $fail")
        Failure(fail)
    }
  }

  private def crawlerUrlEntityBuild(key: com.google.cloud.datastore.Key, dataStoreModel: CrawlerUrlDataStoreModel): Entity = {
    val task: Entity = Entity.newBuilder(key)
      .set("protocol", dataStoreModel.protocol)
      .set("domain", dataStoreModel.domain)
      .set("status", dataStoreModel.status.toString)
      .set("access_time", Timestamp.now())
      .set("tryCount", dataStoreModel.tryCount)
      .build()
    task
  }

  private def crawlerUrlBuildFromEntity(entity: Entity): Option[CrawlerUrlDataStoreModel] = {
    Try {
      val key: Key = entity.getKey
      val id: String = key.getName
      val protocol: String = entity.getString("protocol")
      val domain: String = entity.getString("domain")
      val tryCount: Int = entity.getLong("tryCount").toInt
      val status: StatusKey.Value = StatusKey.values.find(_.toString == entity.getString("status")).getOrElse(StatusKey.Unknown)
      CrawlerUrlDataStoreModel(id, protocol, domain, status, tryCount)
    } match {
      case Success(value) =>
        Some(value)
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Error while map entity to crawler url model. Entity $entity. Fail: $fail")
        None
    }
  }

  def getDataByUrlHostWithPath(hostWithPath: String): Future[Option[CrawlerUrlDataStoreModel]] = {
    implicit val executor: ExecutionContext = ExecutionContexts.dbReadOperations
    dataStoreClient.getData(dataStore, keyFactory, hostWithPath) map {
      case Some(entity: Entity) => crawlerUrlBuildFromEntity(entity)
      case None => None
    } recoverWith {
      case NonFatal(fail) =>
        Logger.error(s"Url $hostWithPath. Error while getting data from DataStore. Fail: $fail")
        Future.failed(fail)
    }
  }

  def setParentStatus(crawlerUrlDataStoreModel: CrawlerUrlDataStoreModel, status: StatusKey.Value, tryCount: Int): Future[Unit] = {
    implicit val executor: ExecutionContext = ExecutionContexts.dbWriteOperations
    val statusChangedModel = crawlerUrlDataStoreModel.copy(status = status, tryCount = tryCount)
    createInstanceThenUpsert(statusChangedModel)
  }

  def setParentStatus(crawlerUrlDataStoreModel: CrawlerUrlDataStoreModel, status: StatusKey.Value): Future[Unit] = {
    implicit val executor: ExecutionContext = ExecutionContexts.dbWriteOperations
    val statusChangedModel = crawlerUrlDataStoreModel.copy(status = status)
    createInstanceThenUpsert(statusChangedModel)
  }

  private def createInstanceThenUpsert(crawlerUrlDataStoreModel: CrawlerUrlDataStoreModel): Future[Unit] = {
    implicit val executor: ExecutionContext = ExecutionContexts.dbWriteOperations
    val data: Entity = createDataStoreInstance(crawlerUrlDataStoreModel)
    dataStoreClient.upsertData(dataStore, data) recoverWith {
      case NonFatal(fail) =>
        Logger.error(s"Url: ${crawlerUrlDataStoreModel.id}. Error while set the status of the parent url $crawlerUrlDataStoreModel")
        Future.failed(fail)
    }
  }

  def incrementFailedCount(data: CrawlerUrlDataStoreModel): CrawlerUrlDataStoreModel =
    data.copy(tryCount = data.tryCount + 1)


  private def createDataStoreInstance(data: CrawlerUrlDataStoreModel): Entity = {
    val key: Key = dataStoreClient.createKey(keyFactory, data.id)
    crawlerUrlEntityBuild(key, data)
  }

  def convertUrlModelToDataStoreModel(links: List[UrlModel], tryCount: Int, status: StatusKey.Value): List[CrawlerUrlDataStoreModel] = {
    links.map(childUrl => CrawlerUrlDataStoreModel(childUrl.hostWithPath, childUrl.protocol, childUrl.domain,
      status, tryCount))
  }

  def convertUrlModelToDataStoreModel(links: List[UrlModel]): List[CrawlerUrlDataStoreModel] = {
    links.map(childUrl => CrawlerUrlDataStoreModel(childUrl.hostWithPath, childUrl.protocol, childUrl.domain,
      StatusKey.NotCrawled, 0))
  }

  def convertUrlModelToDataStoreModel(urlModel: UrlModel, tryCount: Int, status: StatusKey.Value): CrawlerUrlDataStoreModel = {
    CrawlerUrlDataStoreModel(urlModel.hostWithPath, urlModel.protocol, urlModel.domain, status, tryCount)
  }
}
