package core.helpers

import com.google.inject.{Inject, Singleton}
import dispatchers.WebsiteDownloaderExecutor

@Singleton
class DownloadPageHelper @Inject ()(implicit executionContext: WebsiteDownloaderExecutor){
  def downloadPage(url: String): String = scala.io.Source.fromURL(url, "ISO-8859-1").mkString
}
