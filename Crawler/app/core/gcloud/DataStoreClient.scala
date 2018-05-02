package core.gcloud

import javax.inject.{Inject, Singleton}

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.datastore._
import java.io.FileInputStream

import dispatchers.Contexts
import play.Logger
import play.api.Configuration
import play.api.inject.ApplicationLifecycle

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

@Singleton
class DataStoreClient @Inject()(applicationLifecycle: ApplicationLifecycle, config: Configuration, contexts: Contexts) {

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

  def insertData(dataStore: Datastore, dataList: List[Entity]): Future[Unit] = {
    implicit val executor: ExecutionContext = contexts.dbWriteOperations
    Future {
      Try[Unit] {
        dataStore.add(dataList: _*)
        Success((): Unit)
      } recover {
        case e: com.google.cloud.datastore.DatastoreException if e.getCode == 6 =>
          Logger.debug(s"Entity already exist error while inserting to data store $dataList. Exception $e")
          Success((): Unit)
        case NonFatal(exc) =>
          Logger.error(s"Exception while insert data to dataStore $dataList. Exception $exc")
          Failure(exc)
      }
    }
  }

  def upsertData(dataStore: Datastore, data: Entity): Future[Unit] = {
    implicit val executor: ExecutionContext = contexts.dbWriteOperations
    Future {
      Try {
        dataStore.put(data)
      } match {
        case Success(_) => Success((): Unit)
        case Failure(NonFatal(fail)) =>
          Logger.error(s"Error while upserting data to dataStore. Fail: $fail")
          Failure(fail)
      }
    }
  }

  def createKey(keyFactory: KeyFactory, id: String): Key = {
    keyFactory.newKey(id)
  }

  def getData(dataStore: Datastore, keyFactory: KeyFactory, key: String): Future[Option[Entity]] = {
    Try {
      val taskKey: Key = createKey(keyFactory, key)
      val item: Entity = dataStore.get(taskKey, ReadOption.eventualConsistency())
      Logger.debug(s"Data store item get: $item")
      Option(item)
    } match {
      case Success(value) =>
        Future.successful(value)
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Error while getting data from dataStore. Key $key, dataStore: $dataStore")
        Future.failed(fail)
    }
  }

  def deleteEntity(key: String, dataStore: Datastore, keyFactory: KeyFactory): Future[Unit] = {
    implicit val executor: ExecutionContext = contexts.dbWriteOperations
    Future {
      Try {
        val taskKey: Key = createKey(keyFactory, key)
        dataStore.delete(taskKey)
      } match {
        case Success(_) => Success((): Unit)
        case Failure(NonFatal(fail)) =>
          Logger.error("Error while delete data from dataStore")
          Failure(fail)
      }
    }
  }
}
