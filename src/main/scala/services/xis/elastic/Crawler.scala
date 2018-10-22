package services.xis.elastic

import scala.collection.mutable.{Map => MMap}

import services.xis.crawl.ConnectUtil._
import services.xis.crawl.LoginUtil._
import services.xis.crawl.CrawlUtil._
import services.xis.crawl.Article

object Crawler {
  private val pages: Stream[Int] = 1 #:: pages.map(_ + 1)

  private def getArt(bid: (String, String))
    (implicit cookies: Cookie): Option[Article] = bid match {
    case (board, id) => getArticle(board, id)
  }

  def articles: Stream[Article] = {
    implicit val cookies: Cookie = MMap()
    login

    getMaxOfToday match {
      case None => Stream()
      case Some(max) =>
        pages
          .take(max)
          .flatMap(getIdsFromToday)
          .filterNot(_._1 == "seminar_events")
          .flatMap(getArt)
    }
  }
}
