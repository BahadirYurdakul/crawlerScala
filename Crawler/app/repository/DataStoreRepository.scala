package repository

import javax.inject.{Inject, Singleton}

import com.google.cloud.datastore.{Datastore, Entity, KeyFactory}
import core.gcloud.DataStoreClient
import core.utils.{StatusKey, UrlModel}
import models.{DataStoreEntity, DataStoreModel}
import play.Logger
import play.api.Configuration

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class DataStoreRepository @Inject()(dataStoreClient: DataStoreClient, dataStoreEntity: DataStoreEntity
                                    , statusKey: StatusKey
                                    , config: Configuration)
                                   (implicit executionContext: ExecutionContext) {

  private val dataStore: Datastore = dataStoreClient.getDataStore(config.get[String]("crawlerProjectId"))
  private val keyFactory: KeyFactory = dataStoreClient.getKeyFactory(dataStore, config.get[String]("crawlerUrlKindForDataStore"))

  def getDataStore(projectId: String): Datastore = {
    dataStoreClient.getDataStore(projectId)
  }

  def getKeyFactory(dataStore: Datastore, kindName: String): KeyFactory = {
    dataStore.newKeyFactory.setKind(kindName)
  }

  def insertToDataStore(url: String, dataStoreModelList: List[DataStoreModel]): Future[Unit] = {
    val dataList: List[Entity] = dataStoreModelList.map(dataStoreModel => createDataStoreInstance(dataStoreModel))
    dataStoreClient.insertData(dataStore, dataList) recoverWith {
      case fail: Throwable =>
        Logger.error(s"Url: $url. Cannot insert dataList to dataStore. Data $dataStoreModelList Fail $fail")
        throw fail
    }
  }

  def getDataByUrlHostWithPath(hostWithPath: String): Future[Entity] = {
    dataStoreClient.getData(dataStore, keyFactory, hostWithPath) recover {
      case fail: Throwable =>
        Logger.error(s"Url: $hostWithPath. Cannot get data from dataStore by its url. Fail $fail")
        throw fail
    }
  }

  def setParentStatus(url: UrlModel, statusKey: String): Future[Unit] = {
    val data: Entity = createDataStoreInstance(DataStoreModel(url.hostWithPath, url.protocol, url.domain, statusKey))
    dataStoreClient.upsertData(dataStore, data) recover {
      case fail: Throwable =>
        Logger.error(s"Url: ${url.hostWithPath}. Cannot change the status of parent. Fail $fail")
        throw fail
    }
  }

  def createDataStoreInstance(data: DataStoreModel): Entity = {
    val key = dataStoreClient.createKey(keyFactory, data.id)
    dataStoreEntity.createInstance(key, data)
  }

  def convertUrlModelToDataStoreModel(links: List[UrlModel]): List[DataStoreModel] = {
    links.map(
      childUrl => DataStoreModel(childUrl.hostWithPath, childUrl.protocol, childUrl.domain, statusKey.notCrawled)
    )
  }
}
