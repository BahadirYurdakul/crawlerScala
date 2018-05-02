package services

import javax.inject.{Inject, Singleton}

import com.google.cloud.datastore.Entity
import core.gcloud.{CloudStorageClient, PubSubClient}
import core.UrlHelper
import core.helpers.{DownloadPageHelper, ScrapeLinksHelper}
import core.utils.{StatusKey, UrlModel, UrlParseException}
import dispatchers.Contexts
import models.RequestUrl
import play.Logger
import play.api.Configuration
import play.api.libs.json.JsValue
import repository.{CrawlerUrlDataStoreModel, CrawlerUrlDataStoreRepository}

import scala.concurrent._
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

case class IllegalToCrawlUrlException(private val message: String = "", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

case class DataStoreUrlStatusWithTryCount(status: StatusKey.Value, failedCount: Int)
case class DataStoreWithUrlModel(urlModel: UrlModel, crawlerUrlDataStoreModel: CrawlerUrlDataStoreModel)

@Singleton
class CrawlerService @Inject()(downloadPageHelper: DownloadPageHelper, dataStoreRepository: CrawlerUrlDataStoreRepository,
                               pubSubClient: PubSubClient,
                               scrapeLinksHelper: ScrapeLinksHelper, config: Configuration,
                               crawlerUrlRepository: CrawlerUrlDataStoreRepository,
                               cloudStorageClient: CloudStorageClient,
                               contexts: Contexts,
                               urlHelper: UrlHelper) {

  private val crawlerProjectId: String = config.get[String]("crawlerProjectId")
  private val crawlerPubSubTopicName: String = config.get[String]("crawlerPubSubTopicName")
  private val bucketName: String = config.get[String]("crawlerBucket")
  private val maxTryCountForUrl: Int = config.getOptional[Int]("maxTryCountForUrl").getOrElse(10)

  def crawl(url: String): Future[Unit] = {
    implicit val gcloudExecutor: ExecutionContext = contexts.gcloudOperations
    val initialDataStoreParentData: Future[DataStoreWithUrlModel] = for {
      parsedUrl <- Future.fromTry(UrlModel.parse(url, urlHelper))
      parentData: CrawlerUrlDataStoreModel <- getDataStoreDataAndCheckCrawlNeeded(parsedUrl)
      _ <- dataStoreRepository.setParentStatus(parentData, StatusKey.InProgress)
    } yield DataStoreWithUrlModel(parsedUrl, parentData)

    initialDataStoreParentData.flatMap( data =>
      scrapeLinksUploadToStorageAndRecordIntoDataStore(url, data.crawlerUrlDataStoreModel, data.urlModel)
    )(gcloudExecutor) recoverWith {
      case exc@(IllegalToCrawlUrlException(_, _) | UrlParseException(_, _)) =>
        Logger.error(s"Url $url. Non-recoverable exception found. Fail $exc")
        Future.successful((): Unit)
      case NonFatal(fail) =>
        Logger.error(s"Url: $url. Error while recoverable process. Fail: $fail")
        Future.failed(fail)
      }
  }

  def scrapeLinksUploadToStorageAndRecordIntoDataStore(url: String, parentDataStoreData: CrawlerUrlDataStoreModel,
                                                       parsedParent: UrlModel): Future[Unit] = {
    implicit val gcloudExecutor: ExecutionContext = contexts.gcloudOperations

    (for {
      rawHtml <- downloadPageAndUploadToCloudStorage(url)
      result <- extractUrlsThenRecordThem(parsedParent, parentDataStoreData, rawHtml)
    } yield result)(gcloudExecutor) recoverWith {
      case fail: Throwable =>
        Logger.error(s"Url: $url. Error while crawling. Fail $fail")
        dataStoreRepository.setParentStatus(parentDataStoreData, StatusKey.NotCrawled)
        Future.failed(fail)
    }
  }

  def getDataStoreDataAndCheckCrawlNeeded(parsedUrl: UrlModel): Future[CrawlerUrlDataStoreModel] = {
    implicit val gcloudExecutor: ExecutionContext = contexts.gcloudOperations

    dataStoreRepository.getDataByUrlHostWithPath(parsedUrl.hostWithPath) map {
      case Some(value) =>
        if (isCrawlNeeded(value)) dataStoreRepository.incrementFailedCount(value)
        else throw IllegalToCrawlUrlException(s"Url ${parsedUrl.hostWithPath}. Illegal to crawl url. Status ${value.status}." +
          s" Failed Count: ${value.tryCount}")
      case None => dataStoreRepository.convertUrlModelToDataStoreModel(parsedUrl, 1, StatusKey.NotCrawled)
    }
  }

  def extractChildUrls(parentParsedUrl: UrlModel, rawHtml: String): List[UrlModel] = {
    val childUrls: List[UrlModel] = scrapeLinksHelper.scrapeLinks(parentParsedUrl.protocolWithHostWithPath, rawHtml) match {
      case Success(value: List[UrlModel]) => value
      case Failure(NonFatal(fail)) =>
        Logger.error(s"Url: ${parentParsedUrl.hostWithPath}. Error while scrape links from html.")
        throw fail
    }
    urlHelper.extractSameDomainLinks(parentParsedUrl, childUrls)
  }

  def isCrawlNeeded(crawlerUrlDataStoreModel: CrawlerUrlDataStoreModel): Boolean = {
    val status: StatusKey.Value = crawlerUrlDataStoreModel.status
    val failedCount: Int = crawlerUrlDataStoreModel.tryCount

    if (status == StatusKey.Done || status == StatusKey.InProgress || failedCount >= maxTryCountForUrl)
      false
    else
      true
  }

  def extractUrlsThenRecordThem(parentParsedUrl: UrlModel, parentDataStoreData: CrawlerUrlDataStoreModel,
                                rawHtml: String): Future[Unit] = {

    implicit val gcloudExecutor: ExecutionContext = contexts.gcloudOperations
    val childLinks: List[UrlModel] = extractChildUrls(parentParsedUrl, rawHtml)
    val childDataStoreEntities: List[CrawlerUrlDataStoreModel] = dataStoreRepository.convertUrlModelToDataStoreModel(childLinks)
    val messageList: List[JsValue] = childLinks.map(childLink => RequestUrl.jsonBuildWithUrl(childLink.protocolWithHostWithPath))

    (for {
      _ <- dataStoreRepository.insertToDataStore(parentParsedUrl.hostWithPath, childDataStoreEntities)
      //_ <- pubSubClient.publishToPubSub(crawlerProjectId, crawlerPubSubTopicName, messageList)
      out <- dataStoreRepository.setParentStatus(parentDataStoreData, StatusKey.Done)
    } yield out)(contexts.gcloudOperations) recover {
      case NonFatal(fail) =>
        Logger.error(s"Url: ${parentParsedUrl.hostWithPath}. Error while extract url, record and publish. Fail $fail")
        throw fail
    }
  }

  def downloadPageAndUploadToCloudStorage(url: String): Future[String] = Future (
    (for {
      encodedUrl <- urlHelper.encodeUrl(url)
      rawHtml <- downloadPageHelper.downloadPage(url)
      _ <- cloudStorageClient.zipThenUploadToCloudStorage(encodedUrl, rawHtml, bucketName)
    } yield rawHtml) match {
      case Success(value) =>
        value
      case Failure(fail) =>
        Logger.error(s"Url: $url. Error while download page and upload to cloud storage. Fail $fail")
        throw fail
    }
  )(contexts.gcloudOperations)
}
