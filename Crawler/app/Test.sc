
import play.Logger

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

println("fabjkfafa")

def a()(implicit executionContext: ExecutionContext): Future[Option[String]] = Future {
  println("afbakfakjk")
  Some("fagagafad")
}

def c(d: String)(implicit executionContext: ExecutionContext): Future[Either[Throwable,String]] = {
  //throw new Exception
  Right(d)
}

def e(d: String)(implicit executionContext: ExecutionContext): Either[Throwable,String] = {
  //throw new Exception
  Right(d)
}

def fhauhwak(): Option[String] = {
  Some("fafA")
}

def deneme(): Future[Either[Throwable, String]]

class afafa(implicit executionContext: ExecutionContext){
  println("fanfkjakjfankf")

  val desired: Future[Either[Throwable,String]] = c("gfaga").map(fnakfl => fnakfl.right.map(_.toLowerCase))
  val b = desired.right.map(th => Logger.error(s"$th"))
}
