package core.gcloud

import javax.inject.{Inject, Singleton}

import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageOptions}
import core.helpers.ZipHelper
import dispatchers.ExecutionContexts
import play.Logger

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@Singleton
class CloudStorageClient@Inject ()()(zipHelper: ZipHelper) {

  private val storage: Storage = StorageOptions.getDefaultInstance.getService

  def uploadToCloudStorage(fileName: String, content: Array[Byte], bucketName: String): Try[Unit] = {
    val blobInfo: BlobInfo = BlobInfo
      .newBuilder(BlobId.of(bucketName, fileName))
      .setContentType("application/zip").build()

    Try {
      storage.create(blobInfo, content)
      Logger.debug(s"FileName: $fileName. Content uploaded to cloud storage successfully. " +
        s"BucketName $bucketName. Content $content")
    } match {
      case Success(_) =>
        Success((): Unit)
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Filename: $fileName, bucketName: $bucketName. Error while upload data to cloud storage. Content: $content. Fail $fail")
        Failure(fail)
    }
  }

  def zipThenUploadToCloudStorage(filename: String, content: String, bucketName: String): Try[String] = {
    (for {
      compressed <- zipHelper.zipContent(filename, content)
      _ <- uploadToCloudStorage(filename, compressed, bucketName)
    } yield compressed) match {
      case Success(_) =>
        Success(content)
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Filename: $filename. BucketName: $bucketName Error while zip and upload to cloud storage.")
        throw fail
    }
  }
}
