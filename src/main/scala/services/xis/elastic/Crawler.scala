package services.xis.elastic

import scala.collection.mutable.{Map => MMap}

import services.xis.crawl.ConnectUtil._
import services.xis.crawl.LoginUtil._
import services.xis.crawl.CrawlUtil._
import services.xis.crawl.{Article, ArticleSummary}

import services.xis.extractor.Extractor

class Crawler {
  private implicit val cookies: Cookie = MMap()
  login

  private val pages: Stream[Int] = 1 #:: pages.map(_ + 1)

  def article(board: String, id: String): Option[(Article, String, String)] =
    getArticle(board, id).map{ art =>
      val attached = (art.files zip art.links).map{ case (file, link) =>
        val name =
          (if (file.endsWith(" KB)") || file.endsWith(" MB)"))
            file.substring(0, file.lastIndexOf(" ("))
          else file)
        if (Extractor.available(name)) Extractor.extract(name, getFile(link))
        else { println(name); "" }
      }.mkString(" ").replace("\n", " ")

      val image = art.images.map(link =>
        Extractor.extract(link.replace("_", "."), getFile(link))
      ).mkString(" ").replace("\n", " ")

      (art, attached, image)
    }

  def summaries: Stream[ArticleSummary] = {
    getMaxOfToday match {
      case None => Stream()
      case Some(max) => pages.take(max).flatMap(getSummariesFromToday)
    }
  }
}
