package core.helpers

import java.io._
import java.util.zip._
import javax.inject.Singleton

import play.api.Logger

import scala.util.Try
import scala.util.control.NonFatal

@Singleton
class ZipHelper {
  def zipContent(filename: String, content: String): Try[Array[Byte]] = Try {
    val arrOutputStream: ByteArrayOutputStream = new ByteArrayOutputStream(content.length)
    val zipOutStream: ZipOutputStream = new ZipOutputStream(arrOutputStream)

    if(filename.length > 65000)
      throw new IllegalArgumentException(s"Filename $filename too long")
    val zipEntry: ZipEntry = new ZipEntry(filename + ".txt")

    val compressedContent: Array[Byte] = try {
      zipOutStream.putNextEntry(zipEntry)
      zipOutStream.write(content.getBytes())
      arrOutputStream.toByteArray
    } catch {
      case NonFatal(fail) =>
        Logger.error(s"Filename $filename. Error occurred while zipping content $content")
        throw fail
    } finally {
      zipOutStream.closeEntry()
      zipOutStream.close()
      arrOutputStream.close()
    }

    compressedContent
  }
}
