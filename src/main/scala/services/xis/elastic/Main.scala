package services.xis.elastic

import services.xis.crawl.ArticleSummary

object Main {
  def main(args: Array[String]): Unit = {
    val indexer = new Indexer(
      "localhost", 9200, 9300, "http",
      "portal4", "article"
    )
    val crawler = new Crawler

    indexer.createIndex
    args.toList match {
      case "-s" :: Nil => while (true) run(indexer, crawler)
      case Nil => run(indexer, crawler)
      case _ =>
    }
    indexer.close
  }

  def run(indexer: Indexer, crawler: Crawler): Unit = {
    crawler.summaries foreach {
      case ArticleSummary(board, id, hits) =>
        if (indexer.articleExists(id))
          indexer.updateHits(id, hits)
        else
          new Thread() {
            override def run(): Unit = {
              crawler.article(board, id) match {
                case Some((art, att, img)) =>
                  indexer.indexArticle(art, att, img)
                case _ =>
              }
            }
          }.start
    }
  }
}
