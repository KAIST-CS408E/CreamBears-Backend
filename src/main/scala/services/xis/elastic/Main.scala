package services.xis.elastic

import scala.collection.mutable.{Map => MMap}
import scala.io.StdIn

import akka.actor.ActorSystem

import services.xis.elastic.actors.CrawlManager
import services.xis.crawl.ConnectUtil.Cookie

object Main {
  private val host = "localhost"
  private val port0 = 9200
  private val port1 = 9300
  private val protocol = "http"
  private val index = "portal2"
  private val typ = "article"

  def main(args: Array[String]): Unit = {
    val indexer = new Indexer(host, port0, port1, protocol, index, typ)
    indexer.createIndex()

    val system = ActorSystem("index-system")

    val manager = args.toList match {
      case "--debug" :: start :: end :: Nil =>
        system.actorOf(
          CrawlManager.debugProps(
            start = start.toInt,
            end = end.toInt,
            indexer = indexer,
            maxWorkerNum = 1,
            summaryWorkerNum = 3,
            articleWorkerNum = 20,
            fileWorkerNum = 5,
            extractWorkerNum = 50,
            readWorkerNum = 1,
            writeWorkerNum = 10))
      case _ =>
        system.actorOf(
          CrawlManager.props(
            indexer = indexer,
            maxWorkerNum = 1,
            summaryWorkerNum = 3,
            articleWorkerNum = 20,
            fileWorkerNum = 5,
            extractWorkerNum = 50,
            readWorkerNum = 1,
            writeWorkerNum = 10))
    }
    manager ! CrawlManager.Start

    try {
      while (StdIn.readLine().toLowerCase != "exit")
        manager ! CrawlManager.Info
    } finally {
      system.terminate()
      indexer.close()
    }
  }
}
