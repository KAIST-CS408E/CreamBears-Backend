package services.xis.elastic.actors

import akka.actor.{Actor, ActorLogging, Props}

import services.xis.crawl.ConnectUtil.Cookie
import services.xis.crawl.CrawlUtil.getArticle
import services.xis.crawl.Article

object ArticleGetter {
  def props(implicit cookie: Cookie): Props = Props(new ArticleGetter)

  private[this] var id = -1
  private def getId: Int = {
    id += 1
    id
  }

  case class Request(requestId: Int, board: String, id: String)
  case class Success(
    requestId: Int, board: String, id: String, article: Article
  )
  case class Failure(requestId: Int, board: String, id: String)
}

class ArticleGetter(implicit cookie: Cookie) extends Actor with ActorLogging {
  import ArticleGetter._

  val id: Int = getId

  override def preStart(): Unit = {
    log.info("ArticleGetter:{} starts", id)
  }

  override def postStop(): Unit = {
    log.info("ArticleGetter:{} stops", id)
  }

  override def receive: Receive = {
    case Request(requestId, board, aid) =>
      log.info("ArticleGetter:{} receives request:{} for {}/{}",
        id, requestId, board, aid)
      getArticle(board, aid) match {
        case Some(article) =>
          log.info("ArticleGetter:{} succeeds for for {}/{} of request:{}",
			      id, board, aid, requestId)
          sender() ! Success(requestId, board, aid, article)
        case None =>
          log.warning("ArticleGetter:{} fails for for {}/{} of request:{}",
			      id, board, aid, requestId)
          sender() ! Failure(requestId, board, aid)
      }
  }
}
