package core.utils

import javax.inject.Singleton

@Singleton
class StatusKey {
  val done = "Done"
  val notCrawled = "Not Crawled Yet"
  val notFound = "Not Found"
  val inProgress = "In Progress"
}
