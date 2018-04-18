package services

import javax.inject.{Inject, Singleton}

import com.google.cloud.datastore.Entity
import core.gcloud.{CloudStorageClient, PubSubClient}
import core.UrlHelper
import core.helpers.{DownloadPageHelper, ScrapeLinksHelper, ZipHelper}
import core.utils.{StatusKey, UrlModel}
import models.{DataStoreModel, RequestUrl}
import play.Logger
import play.api.Configuration
import play.api.libs.json.JsValue
import repository.{CloudStorageRepository, DataStoreRepository}

import scala.concurrent._
import scala.util.{Failure, Success}

@Singleton
class CrawlerService @Inject()(downloadPageHelper: DownloadPageHelper, dataStoreRepository: DataStoreRepository
                               , statusKey: StatusKey, pubSubClient: PubSubClient
                               , scrapeLinksHelper: ScrapeLinksHelper, config: Configuration
                               , cloudStorageClient: CloudStorageClient
                               , cloudStorageRepository: CloudStorageRepository
                               , zipHelper: ZipHelper
                               , urlHelper: UrlHelper)
                              (implicit executionContext: ExecutionContext) {

  val crawlerProjectId: String = config.getOptional[String]("crawlerProjectId").getOrElse("crawlernode")
  val crawlerPubSubTopicName: String = config.getOptional[String]("crawlerPubSubTopicName").getOrElse("scalaCrawler")

  def crawl(url: String): Future[Unit] = {
    val parentParsedUrl: UrlModel = UrlModel(url, urlHelper) match {
      case Success(value: UrlModel) => value
      case Failure(fail: Throwable) =>
        Logger.error(s"Url: $url. Error while getting url model. Fail $fail")
        return Future.successful((): Unit)
    }
    val encodedUrl: String = urlHelper.encodeUrl(url) match {
      case Success(value: String) => value
      case Failure(fail: Throwable) =>
        Logger.error(s"Url: ${parentParsedUrl.hostWithPath}. Error while encoding url. Fail $fail")
        return Future.successful((): Unit)
    }

    dataStoreRepository.getDataByUrlHostWithPath(parentParsedUrl.hostWithPath) map { parentEntity =>
      determineCrawlOrNot(url, parentEntity)
    } flatMap  { _ => dataStoreRepository.setParentStatus(parentParsedUrl, statusKey.inProgress)
    } map  { _ => downloadPageHelper.downloadPage(url)
    } flatMap  { rawHtml => cloudStorageRepository.uploadToCloudStorage(encodedUrl, rawHtml)
    } flatMap  { rawHtml => extractUrlsAndRecordThem(parentParsedUrl, rawHtml)
    } flatMap { _ => dataStoreRepository.setParentStatus(parentParsedUrl, statusKey.done)
    } recoverWith {
      case _: IllegalToCrawlUrlException =>
        Logger.debug(s"Url: ${parentParsedUrl.hostWithPath}. Url is already crawled")
        Future.successful((): Unit)
      case fail: Throwable =>
        Logger.error(s"Url: ${parentParsedUrl.hostWithPath}. Error while crawling url. Fail: $fail")
        dataStoreRepository.setParentStatus(parentParsedUrl, statusKey.notCrawled)
        Future.failed(fail)
    }
  }

  def extractChildUrls(parentParsedUrl: UrlModel, rawHtml: String): List[UrlModel] = {
    val childUrls: List[UrlModel] = scrapeLinksHelper.scrapeLinks(parentParsedUrl.protocolWithHostWithPath, rawHtml) match {
      case Success(value) => value
      case Failure(fail) =>
        Logger.error(s"Url: ${parentParsedUrl.hostWithPath}. Error while scrape links from html.")
        throw fail
    }
    extractSameDomainLinks(parentParsedUrl, childUrls)
  }

  def extractSameDomainLinks(parentParsedUrl: UrlModel, links: List[UrlModel]): List[UrlModel] = {
    links.flatMap(childUrl => {
      if(childUrl.domain == parentParsedUrl.domain && parentParsedUrl.hostWithPath != childUrl.hostWithPath)
        Some(childUrl)
      else
        None
    })
  }

  @throws(classOf[IllegalToCrawlUrlException])
  def determineCrawlOrNot(url: String, parentEntity: Entity): Unit = {
    if(parentEntity == null)
      return
    val status: String = parentEntity.getString("status")
    if(status == null)
      return
    if(status == statusKey.done) {
      Logger.debug(s"Url: $url. Url is already crawled.")
      throw IllegalToCrawlUrlException(s"Url: $url. Url is already crawled.")
    }
    else if(status == statusKey.inProgress) {
      Logger.debug(s"Url: $url. Url is in progress.")
      throw IllegalToCrawlUrlException(s"Url: $url. Url is in progress.")
    }
  }

  def extractUrlsAndRecordThem(parentParsedUrl: UrlModel, rawHtml: String): Future[Unit] = {
    val childLinks: List[UrlModel] = extractChildUrls(parentParsedUrl, rawHtml)
    val childDataStoreEntities: List[DataStoreModel] = dataStoreRepository.convertUrlModelToDataStoreModel(childLinks)

    dataStoreRepository.insertToDataStore(parentParsedUrl.hostWithPath, childDataStoreEntities) map { _ =>
      val messageList: List[JsValue] = childLinks.map(childLink =>
        RequestUrl.jsonBuildWithUrl(childLink.protocolWithHostWithPath)
      )
      //pubSubClient.publishToPubSub(crawlerProjectId, crawlerPubSubTopicName, messageList)
    } recoverWith {
      case fail: Throwable =>
        Logger.error(s"Url: ${parentParsedUrl.hostWithPath}. Error while extract links and record. Fail: $fail")
        Future.failed(fail)
    }
  }

  case class IllegalToCrawlUrlException(private val message: String = "", private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
}
