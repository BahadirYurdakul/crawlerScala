package core.gcloud

import javax.inject.{Inject, Singleton}

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.datastore._
import java.io.FileInputStream

import dispatchers.GCloudExecutor
import play.Logger
import play.api.Configuration
import play.api.inject.ApplicationLifecycle

import scala.concurrent.Future

@Singleton
class DataStoreClient @Inject()(applicationLifecycle: ApplicationLifecycle, config: Configuration)
                               (implicit executionContext: GCloudExecutor) {

  private val keyFilePath: String = config.getOptional[String]("googleCloudKeyFilePath").getOrElse("keyfile.json")

  def getDataStore(projectId: String): Datastore = {
    val options: DatastoreOptions = DatastoreOptions.newBuilder
      .setProjectId(projectId)
      .setCredentials(GoogleCredentials.fromStream(new FileInputStream(keyFilePath))).build
    options.getService
  }

  def getKeyFactory(dataStore: Datastore, kindName: String): KeyFactory = {
    dataStore.newKeyFactory.setKind(kindName)
  }

  def insertData(dataStore: Datastore, dataList: List[Entity]): Future[Unit] = Future[Unit] {
    try {
      dataStore.add(dataList: _*)
    } catch {
      case e: com.google.cloud.datastore.DatastoreException if e.getCode == 6 =>
        Logger.debug(s"Entity already exist error while inserting to data store $dataList. Exception $e")
      case exc: Throwable =>
        Logger.error(s"Exception while insert data to dataStore $dataList. Exception $exc")
        Future.failed(exc)
    }
  }

  def upsertData(dataStore: Datastore, data: Entity): Future[Unit] = Future[Unit] {
    try {
      dataStore.put(data)
    } catch {
      case fail: Throwable =>
        Logger.error(s"Error while upserting data to dataStore. Fail: $fail")
        Future.failed(fail)
    }
  }

  def createKey(keyFactory: KeyFactory, id: String): Key = {
    keyFactory.newKey(id)
  }

  def getData(dataStore: Datastore, keyFactory: KeyFactory, key: String): Future[Entity] = Future[Entity] {
    try {
      val taskKey: Key = createKey(keyFactory, key)
      val item: Entity = dataStore.get(taskKey, ReadOption.eventualConsistency())
      Logger.debug(s"Data store item get: $item")
      item
    } catch {
      case fail: Throwable =>
        Logger.error(s"Error while get data from dataStore. Fail: $fail ")
        return Future.failed(fail)
    }
  }

  def deleteEntity(key: String, dataStore: Datastore, keyFactory: KeyFactory): Future[Unit] = Future[Unit] {
    try {
      val taskKey: Key = createKey(keyFactory, key)
      dataStore.delete(taskKey)
    } catch {
      case dataStoreException: Throwable =>
        Logger.error("Error while delete data from dataStore")
        Future.failed(dataStoreException)
    }
  }
}
