package services

import javax.inject.{Inject, Singleton}

import com.google.cloud.datastore.Entity
import core.gcloud.{CloudStorageClient, PubSubClient}
import core.UrlHelper
import core.helpers.{DownloadPageHelper, ScrapeLinksHelper}
import core.utils.{StatusKey, UrlModel}
import models.RequestUrl
import play.Logger
import play.api.Configuration
import play.api.libs.json.JsValue
import repository.{CrawlerUrlDataStoreRepository, CrawlerUrlModel}

import scala.concurrent._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

@Singleton
class CrawlerService @Inject()(downloadPageHelper: DownloadPageHelper, dataStoreRepository: CrawlerUrlDataStoreRepository,
                               statusKey: StatusKey, pubSubClient: PubSubClient,
                               scrapeLinksHelper: ScrapeLinksHelper, config: Configuration,
                               crawlerUrlRepository: CrawlerUrlDataStoreRepository,
                               cloudStorageClient: CloudStorageClient,
                               urlHelper: UrlHelper)
                              (implicit executionContext: ExecutionContext) {

  private val crawlerProjectId: String = config.get[String]("crawlerProjectId")
  private val crawlerPubSubTopicName: String = config.get[String]("crawlerPubSubTopicName")
  private val bucketName: String = config.get[String]("crawlerBucket")

  def crawl(url: String): Future[Unit] = {
    val parentParsedUrl: UrlModel = UrlModel.parse(url, urlHelper) match {
      case Success(value: UrlModel) => value
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Url: $url. Error while getting url model. Fail $fail")
        return Future.successful((): Unit)
    }

    isCrawlNeeded(url) map { isNeeded: Boolean =>
      if(!isNeeded) throw IllegalToCrawlUrlException(s"Url: ${parentParsedUrl.hostWithPath}. Url doesn't need to be crawled")
      dataStoreRepository.setParentStatus(parentParsedUrl, statusKey.inProgress)
    } flatMap { _ =>
      downloadPageAndUploadToCloudStorage(url)
    } flatMap { rawHtml =>
      extractUrlsThenRecordThem(parentParsedUrl, rawHtml)
    } recoverWith {
      case _: IllegalToCrawlUrlException =>
        Logger.debug(s"Url: ${parentParsedUrl.hostWithPath}. Url doesn't need to be crawled")
        Future.successful((): Unit)
      case fail: Throwable =>
        Logger.error(s"Url: ${parentParsedUrl.hostWithPath}. Error while crawling url. Fail: $fail")
        dataStoreRepository.setParentStatus(parentParsedUrl, statusKey.notCrawled)
        Future.failed(fail)
    }
  }

  def extractChildUrls(parentParsedUrl: UrlModel, rawHtml: String): List[UrlModel] = {
    val childUrls: List[UrlModel] = scrapeLinksHelper.scrapeLinks(parentParsedUrl.protocolWithHostWithPath, rawHtml) match {
      case Success(value: List[UrlModel]) => value
      case Failure(fail: Throwable) =>
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

  def isCrawlNeeded(url: String): Future[Boolean] = {
    crawlerUrlRepository.getDataStatusByUrlHostWithPath(url) map { status: Option[String] =>
      status match {
        case Some(stat: String) =>
          if(stat == statusKey.done || stat == statusKey.inProgress) {
            false
          } else {
            true
          }
        case None => true
      }
    } recover {
      case NonFatal(fail) =>
        Logger.error(s" Url $url. Error while checking if crawling needed. Fail $fail")
        true
    }
  }

  def extractUrlsThenRecordThem(parentParsedUrl: UrlModel, rawHtml: String): Future[Unit] = {
    val childLinks: List[UrlModel] = extractChildUrls(parentParsedUrl, rawHtml)
    val childDataStoreEntities: List[CrawlerUrlModel] = dataStoreRepository.convertUrlModelToDataStoreModel(childLinks)
    val messageList: List[JsValue] = childLinks.map(childLink =>
      RequestUrl.jsonBuildWithUrl(childLink.protocolWithHostWithPath)
    )

    dataStoreRepository.insertToDataStore(parentParsedUrl.hostWithPath, childDataStoreEntities) map { _ =>
      //pubSubClient.publishToPubSub(crawlerProjectId, crawlerPubSubTopicName, messageList)
    } flatMap { _ =>
      dataStoreRepository.setParentStatus(parentParsedUrl, statusKey.done)
    } recoverWith {
      case fail: Throwable =>
        Logger.error(s"Url: ${parentParsedUrl.hostWithPath}. Error while extract links and record. Fail: $fail")
        Future.failed(fail)
    }
  }

  def downloadPageAndUploadToCloudStorage(url: String): Future[String] = {
    val encodedUrl: String = urlHelper.encodeUrl(url) match {
      case Success(value: String) => value
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Url: $url. Error while encoding url. Fail $fail")
        return Future.failed(fail)
    }

    val rawHtml: String = downloadPageHelper.downloadPage(url)
    cloudStorageClient.zipThenUploadToCloudStorage(encodedUrl, rawHtml, bucketName)
  }

  case class IllegalToCrawlUrlException(private val message: String = "", private val cause: Throwable = None.orNull)
    extends Exception(message, cause)
}
