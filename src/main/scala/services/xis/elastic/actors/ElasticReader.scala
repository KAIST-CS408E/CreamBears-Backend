package services.xis.elastic.actors

import java.io.IOException

import akka.actor.{Actor, ActorLogging, Props}

import org.elasticsearch.client.{RestHighLevelClient, RequestOptions}
import org.elasticsearch.action.get.GetRequest

import services.xis.elastic.Indexer
import services.xis.crawl.ArticleSummary

object ElasticReader {
  def props(indexer: Indexer): Props = Props(new ElasticReader(indexer))

  private[this] var id = -1
  private def getId: Int = {
    id += 1
    id
  }

  case class Request(requestId: Int, summ: ArticleSummary)
  case class Success(requestId: Int, summ: ArticleSummary, exist: Boolean)
  case class Failure(requestId: Int, summ: ArticleSummary)
}

class ElasticReader(indexer: Indexer) extends Actor with ActorLogging {
  import ElasticReader._

  val id: Int = getId

  override def preStart(): Unit = {
    log.info("ElasticReader:{} starts", id)
  }

  override def postStop(): Unit = {
    log.info("ElasticReader:{} stops", id)
  }

  override def receive: Receive = {
    case Request(requestId, summ) =>
      log.info("ElasticReader:{} receives request:{} for {}",
        id, requestId, summ.id)
      try {
        val exist = indexer.articleExists(summ.id)
        log.info("ElasticReader:{} succeeds for for {} of request:{}",
  	      id, summ.id, requestId)
        sender() ! Success(requestId, summ, exist)
      } catch {
        case e: IOException =>
          log.warning("ElasticReader:{} fails for {} of request:{} - {}",
			      id, summ.id, requestId, e.getMessage)
          sender() ! Failure(requestId, summ)
      }
  }
}
