package core.helpers

import java.io._
import java.util.zip._
import javax.inject.Singleton

import play.api.Logger
import services.IllegalToCrawlUrlException

import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@Singleton
class ZipHelper {

  def zipContent(filename: String, content: String): Try[Array[Byte]] = {
    val arrOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream(content.length)
    val zipOutStream: ZipOutputStream = new ZipOutputStream(arrOutputStream)

    val compressedBytes: Try[Array[Byte]] = Try {
        new ZipEntry(filename + ".txt")
    } match {
      case Success(zipEntry: ZipEntry) =>
        writeIntoZipEntryThenCloseStreams(zipEntry, zipOutStream, arrOutputStream, content)
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Filename $filename. Error while creating zip Entry. Fail: $fail")
        closeZipStreams(zipOutStream)
        Failure(fail)
    }
    safelyCloseArrStream(arrOutputStream)
    compressedBytes
  }


  private def writeIntoZipEntryThenCloseStreams(zipEntry: ZipEntry, zipOutStream: ZipOutputStream,
                                                arrOutStream: ByteArrayOutputStream, content: String): Try[Array[Byte]] = {
    val compressedContent: Try[Array[Byte]] = Try {
      zipOutStream.putNextEntry(zipEntry)
      zipOutStream.write(content.getBytes())
    } match {
      case Success(_) =>
        closeZipStreamAndGetCompressedData(zipOutStream, arrOutStream)
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Error while writing to stream. Fail $fail")
        Failure(fail)
    }
    compressedContent
  }

  private def safelyCloseArrStream(arrOutputStream: ByteArrayOutputStream): Unit = Try {
    arrOutputStream.close()
  } match {
    case Success(_) => Success((): Unit)
    case Failure(NonFatal(fail)) => Logger.error(s"Error while closing array output stream. Fail $fail")
  }

  private def closeZipStreams(zipOutStream: ZipOutputStream): Try[Unit] = Try {
    zipOutStream.closeEntry()
    zipOutStream.close()
  }

  private def closeZipStreamAndGetCompressedData(zipOutStream: ZipOutputStream,
                                                 arrOutputStream: ByteArrayOutputStream): Try[Array[Byte]] =
  closeZipStreams(zipOutStream) match {
    case Success(_) =>
      Success(arrOutputStream.toByteArray)
    case Failure(NonFatal(fail)) =>
      Logger.error(s"Error while closing zip helper streams. Fail $fail")
      Failure(fail)
  }

  /*
def zipContent(filename: String, content: String): Try[Array[Byte]] = Try {
  val arrOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream(content.length)
  val zipOutStream: ZipOutputStream = new ZipOutputStream(arrOutputStream)
  val zipEntry: ZipEntry = new ZipEntry(filename + ".txt")
  zipOutStream.putNextEntry(zipEntry)
  zipOutStream.write(content.getBytes())
  zipOutStream.closeEntry()
  zipOutStream.close()
  val compressedContent: Array[Byte] = arrOutputStream.toByteArray
  arrOutputStream.close()
  compressedContent
}
*/
}
