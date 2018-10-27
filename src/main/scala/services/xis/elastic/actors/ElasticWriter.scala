package services.xis.elastic.actors

import java.io.IOException

import akka.actor.{Actor, ActorLogging, Props}

import org.elasticsearch.client.{RestHighLevelClient, RequestOptions}
import org.elasticsearch.action.index.IndexRequest

import services.xis.elastic.{Indexer, ArticleDocument}
import services.xis.crawl.ArticleSummary

object ElasticWriter {
  def props(indexer: Indexer): Props = Props(new ElasticWriter(indexer))

  private[this] var id = -1
  private def getId: Int = {
    id += 1
    id
  }

  sealed trait Request
  case class CreateRequest(requestId: Int, artDoc: ArticleDocument)
    extends Request
  case class UpdateRequest(requestId: Int, summary: ArticleSummary)
    extends Request
  case class Success(requestId: Int)
  case class CreateFailure(requestId: Int, artDoc: ArticleDocument)
  case class UpdateFailure(requestId: Int, summary: ArticleSummary)
}

class ElasticWriter(indexer: Indexer) extends Actor with ActorLogging {
  import ElasticWriter._

  val id: Int = getId

  override def preStart(): Unit = {
    log.info("ElasticWriter:{} starts", id)
  }

  override def postStop(): Unit = {
    log.info("ElasticWriter:{} stops", id)
  }

  override def receive: Receive = {
    case CreateRequest(requestId, artDoc) =>
      log.info("ElasticWriter:{} receives request:{} for {}",
        id, requestId, artDoc.article.id)
      try {
        indexer.indexArticle(artDoc)
        log.info("ElasticWriter:{} succeeds for for {} of request:{}",
  	      id, artDoc.article.id, requestId)
        sender() ! Success(requestId)
      } catch {
        case e: Exception =>
          log.warning("ElasticWriter:{} fails for {} of request:{} - {}",
			      id, artDoc.article.id, requestId, e.getMessage)
          sender() ! CreateFailure(requestId, artDoc)
      }
    case UpdateRequest(requestId, summ) =>
      log.info("ElasticWriter:{} receives request:{} for {}",
        id, requestId, summ.id)
      try {
        indexer.updateHits(summ)
        log.info("ElasticWriter:{} succeeds for for {} of request:{}",
  	      id, summ.id, requestId)
        sender() ! Success(requestId)
      } catch {
        case e: Exception =>
          log.warning("ElasticWriter:{} fails for {} of request:{} - {}",
			      id, summ.id, requestId, e.getMessage)
          sender() ! UpdateFailure(requestId, summ)
      }
  }
}
