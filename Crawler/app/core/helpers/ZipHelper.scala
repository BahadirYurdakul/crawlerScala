package core.helpers

import java.io._
import java.util.zip._
import javax.inject.Singleton

import scala.util.Try

@Singleton
class ZipHelper {
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
}
