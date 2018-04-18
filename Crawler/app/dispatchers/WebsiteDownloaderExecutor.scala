package dispatchers

import javax.inject.{Inject, Singleton}

import akka.actor.ActorSystem
import play.libs.concurrent.CustomExecutionContext

@Singleton
class WebsiteDownloaderExecutor @Inject()(actorSystem: ActorSystem)
  extends CustomExecutionContext(actorSystem, "websiteDownloader.dispatcher")
