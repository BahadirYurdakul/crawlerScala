package core.gcloud

import javax.inject.{Inject, Singleton}

import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageOptions}
import core.helpers.ZipHelper
import dispatchers.GCloudExecutor

import scala.concurrent.Future
import play.Logger

import scala.util.control.NonFatal
import scala.util.{Failure, Success}

@Singleton
class CloudStorageClient@Inject ()()(implicit executionContext: GCloudExecutor, zipHelper: ZipHelper) {

  private val storage: Storage = StorageOptions.getDefaultInstance.getService

  def uploadToCloudStorage(fileName: String, content: Array[Byte], bucketName: String): Future[Unit] = {
    val blobInfo: BlobInfo = BlobInfo
      .newBuilder(BlobId.of(bucketName, fileName))
      .setContentType("application/zip").build()

    Future[Unit] {
      storage.create(blobInfo, content)
      Logger.debug(s"FileName: $fileName. Content uploaded to cloud storage successfully. " +
        s"BucketName $bucketName. Content $content")
    } recoverWith {
      case NonFatal(fail) =>
        Logger.error(s"FileName: $fileName. Content cannot be uploaded to cloud storage. " +
          s"BucketName $bucketName. Content $content")
        Future.failed(fail)
    }
  }

  def zipThenUploadToCloudStorage(filename: String, content: String, bucketName: String): Future[String] = {
    val compressed: Array[Byte] = zipHelper.zipContent(filename, content) match {
      case Success(value: Array[Byte]) => value
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Filename: $filename. Error while zipping content. Content: $content")
        return Future.failed(fail)
    }

    uploadToCloudStorage(filename, compressed, bucketName) flatMap { _ =>
      Future.successful(content)
    } recoverWith {
      case NonFatal(fail) =>
        Logger.error(s"FileName: $filename. Error while uploading data to cloud storage. Content: $content")
        Future.failed(fail)
    }
  }
}
