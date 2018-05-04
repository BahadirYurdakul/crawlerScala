package dispatchers

import javax.inject.{Inject, Singleton}

import akka.actor.ActorSystem
import play.api.libs.concurrent.CustomExecutionContext

import scala.concurrent.ExecutionContext

@Singleton
class ExecutionContexts @Inject()(actorSystem: ActorSystem) {
  val gcloudOperations: ExecutionContext = actorSystem.dispatchers.lookup("ExecutionContexts.gcloud.operations")
  val dbReadOperations: ExecutionContext = actorSystem.dispatchers.lookup("ExecutionContexts.databaseRead.operations")
  val dbWriteOperations: ExecutionContext = actorSystem.dispatchers.lookup("ExecutionContexts.databaseWrite.operations")
  val downloadWebOperations: ExecutionContext = actorSystem.dispatchers.lookup("ExecutionContexts.websiteDownloader.operations")
  val expensiveCpuOperations: ExecutionContext = actorSystem.dispatchers.lookup("ExecutionContexts.expensiveCpu.operations")
}
