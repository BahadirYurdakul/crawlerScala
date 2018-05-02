package dispatchers

import javax.inject.{Inject, Singleton}

import akka.actor.ActorSystem
import play.api.libs.concurrent.CustomExecutionContext

import scala.concurrent.ExecutionContext

@Singleton
class Contexts @Inject()(actorSystem: ActorSystem) {
  implicit val gcloudOperations: ExecutionContext = actorSystem.dispatchers.lookup("contexts.gcloud.operations")
  implicit val dbReadOperations: ExecutionContext = actorSystem.dispatchers.lookup("contexts.databaseRead.operations")
  implicit val dbWriteOperations: ExecutionContext = actorSystem.dispatchers.lookup("contexts.databaseWrite.operations")
  implicit val downloadWebOperations: ExecutionContext = actorSystem.dispatchers.lookup("contexts.websiteDownloader.operations")
  implicit val expensiveCpuOperations: ExecutionContext = actorSystem.dispatchers.lookup("contexts.expensiveCpu.operations")
}
