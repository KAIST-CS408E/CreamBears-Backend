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

  def main(args: Array[String]): Unit = (args.toList match {
    case index :: typ :: tail =>
      val _indexer = new Indexer(host, port0, port1, protocol, index, typ)
      val system = ActorSystem("index-system")
      val (indexer0, manager) = tail match {
        case "--debug" :: start :: end :: tail1 =>
          val __indexer = tail1 match {
            case startD :: endD :: Nil =>
              _indexer.close()
              new Indexer(
                host, port0, port1, protocol, index, typ, startD, endD
              )
            case _ => _indexer
          }
          (__indexer,
            system.actorOf(
              CrawlManager.debugProps(
                start = start.toInt,
                end = end.toInt,
                indexer = __indexer,
                maxWorkerNum = 1,
                summaryWorkerNum = 3,
                articleWorkerNum = 20,
                fileWorkerNum = 5,
                extractWorkerNum = 50,
                readWorkerNum = 1,
                writeWorkerNum = 10)))
        case _ =>
          (_indexer,
            system.actorOf(
              CrawlManager.props(
                indexer = _indexer,
                maxWorkerNum = 1,
                summaryWorkerNum = 3,
                articleWorkerNum = 20,
                fileWorkerNum = 5,
                extractWorkerNum = 50,
                readWorkerNum = 1,
                writeWorkerNum = 10)))
      }
      Some(system, indexer0, manager)
    case _ => None
  }).map{ case (system, indexer, manager) =>
    indexer.createIndex()
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
