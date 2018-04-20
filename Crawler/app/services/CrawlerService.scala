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

case class IllegalToCrawlUrlException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class StatusWithTryCount(status: StatusKey.Value, failedCount: Int)

@Singleton
class CrawlerService @Inject()(downloadPageHelper: DownloadPageHelper, dataStoreRepository: CrawlerUrlDataStoreRepository,
                               pubSubClient: PubSubClient,
                               scrapeLinksHelper: ScrapeLinksHelper, config: Configuration,
                               crawlerUrlRepository: CrawlerUrlDataStoreRepository,
                               cloudStorageClient: CloudStorageClient,
                               urlHelper: UrlHelper)
                              (implicit executionContext: ExecutionContext) {

  private val crawlerProjectId: String = config.get[String]("crawlerProjectId")
  private val crawlerPubSubTopicName: String = config.get[String]("crawlerPubSubTopicName")
  private val bucketName: String = config.get[String]("crawlerBucket")
  private val maxTryCountForUrl: Int = config.getOptional[Int]("maxTryCountForUrl").getOrElse(10)

  def crawl(url: String): Future[Unit] = {

    val crawlerOperations: Future[Unit] = UrlModel.parse(url, urlHelper) match {
      case Success(parsedUrl: UrlModel) =>
        doCrawlerOperations(parsedUrl, url) recoverWith {
          case _: IllegalToCrawlUrlException =>
            Logger.debug(s"Url: ${parsedUrl.hostWithPath}. Url doesn't need to be crawled")
            Future.successful((): Unit)
          case NonFatal(fail) =>
            Logger.error(s"Url: ${parsedUrl.hostWithPath}. Error while crawling url. Fail: $fail")
            dataStoreRepository.setParentStatus(parsedUrl, StatusKey.NotCrawled)
            Future.failed(fail)
        }
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Url: $url. Error while parsing url. Fail $fail")
        Future.successful((): Unit)
    }
    crawlerOperations
  }

  def doCrawlerOperations(parentParsedUrl: UrlModel, url: String): Future[Unit] = {
    for {
      isNeeded: Boolean <- isCrawlNeeded(parentParsedUrl.hostWithPath)
      _ <- if (!isNeeded) {
        Future.failed(throw IllegalToCrawlUrlException(s"Url: ${parentParsedUrl.hostWithPath}. Url doesn't need to be crawled"))
      } else{
        dataStoreRepository.setParentStatus(parentParsedUrl, StatusKey.InProgress)
      }
      rawHtml: String <- downloadPageAndUploadToCloudStorage(url)
      out <- extractUrlsThenRecordThem(parentParsedUrl, rawHtml)
    } yield out
  }

  def extractChildUrls(parentParsedUrl: UrlModel, rawHtml: String): List[UrlModel] = {
    val childUrls: List[UrlModel] = scrapeLinksHelper.scrapeLinks(parentParsedUrl.protocolWithHostWithPath, rawHtml) match {
      case Success(value: List[UrlModel]) => value
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Url: ${parentParsedUrl.hostWithPath}. Error while scrape links from html.")
        throw fail
    }
    extractSameDomainLinks(parentParsedUrl, childUrls)
  }

  def extractSameDomainLinks(parentParsedUrl: UrlModel, links: List[UrlModel]): List[UrlModel] = {
    links.flatMap(childUrl => {
      if (childUrl.domain == parentParsedUrl.domain && parentParsedUrl.hostWithPath != childUrl.hostWithPath)
        Some(childUrl)
      else
        None
    })
  }

  def isCrawlNeeded(url: String): Future[Boolean] = {
    crawlerUrlRepository.getDataStatusByUrlHostWithPath(url) map { status: Option[StatusKey.Value] =>
      status match {
        case Some(StatusKey.Done) | Some(StatusKey.InProgress) => false
        case _ => true
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
      dataStoreRepository.setParentStatus(parentParsedUrl, StatusKey.Done)
    } recoverWith {
      case NonFatal(fail) =>
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

}
