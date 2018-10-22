package services.xis.elastic

object Main {
  def main(args: Array[String]): Unit = {
    Crawler.articles take 10 foreach { art =>
      println(art.title)
      art.images foreach { i => println(ImageResolver.extractText(i)) }
//      Indexer.index(art)
    }
  }
}
