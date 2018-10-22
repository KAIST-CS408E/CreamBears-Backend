package services.xis.elastic

import scala.collection.mutable.{Map => MMap}

import services.xis.crawl.ConnectUtil._
import services.xis.crawl.LoginUtil._
import services.xis.crawl.CrawlUtil._

object Main {
  def main(args: Array[String]): Unit = {
    implicit val cookies: Cookie = MMap()

    login
    for (
      max <- getMaxOfToday();
      num <- 1 to max;
      bid <- getIdsFromToday(num)
    ) bid match {
      case (board, id) =>
        println(board)
        println(id)
    }
  }
}
