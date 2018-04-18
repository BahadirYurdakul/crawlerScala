package repository

import javax.inject.{Inject, Singleton}

import core.gcloud.CloudStorageClient
import core.helpers.ZipHelper
import play.Logger
import play.api.Configuration

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

@Singleton
class CloudStorageRepository @Inject()()(implicit executionContext: ExecutionContext, zipHelper: ZipHelper
                                         , cloudStorageClient: CloudStorageClient
                                         , config: Configuration) {

  private val bucketName: String = config.getOptional[String]("crawlerBucket").getOrElse("crawler_bucket")

  def uploadToCloudStorage(encodedUrl: String, rawHtml: String): Future[String] = {
    val compressed: Array[Byte] = zipHelper.zipContent(encodedUrl, rawHtml) match {
      case Success(value) => value
      case Failure(fail) =>
        Logger.error(s"Encoded Url: $encodedUrl. Error while zipping content. Content: $rawHtml")
        return Future.failed(fail)
    }
    cloudStorageClient.uploadToCloudStorage(encodedUrl, compressed, bucketName) flatMap { _ =>
      Future.successful(rawHtml)
    } recoverWith {
      case fail: Throwable =>
        Logger.error(s"FileName: $encodedUrl. Error while uploading data to cloud storage. Content: $rawHtml")
        Future.failed(fail)
    }
  }

}
