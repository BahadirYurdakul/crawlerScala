package core.gcloud

import javax.inject.{Inject, Singleton}

import com.google.cloud.storage.{BlobId, BlobInfo, StorageOptions}
import dispatchers.GCloudExecutor

import scala.concurrent.Future
import play.Logger

@Singleton
class CloudStorageClient@Inject ()()(implicit executionContext: GCloudExecutor) {

  private val storage = StorageOptions.getDefaultInstance.getService

  def uploadToCloudStorage(fileName: String, content: Array[Byte], bucketName: String): Future[Unit] = {
    val blobInfo = BlobInfo
      .newBuilder(BlobId.of(bucketName, fileName))
      .setContentType("application/zip").build()
    Future[Unit] {
      storage.create(blobInfo, content)
      Logger.debug(s"FileName: $fileName. Content uploaded to cloud storage successfully. " +
        s"BucketName $bucketName. Content $content")
    } recoverWith {
      case fail: Throwable =>
        Logger.error(s"FileName: $fileName. Content cannot be uploaded to cloud storage. " +
          s"BucketName $bucketName. Content $content")
        Future.failed(fail)
    }
  }
}
