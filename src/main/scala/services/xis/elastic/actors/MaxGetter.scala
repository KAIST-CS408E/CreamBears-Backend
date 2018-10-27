package services.xis.elastic.actors

import akka.actor.{Actor, ActorLogging, Props}

import services.xis.crawl.ConnectUtil.Cookie
import services.xis.crawl.CrawlUtil.getMaxOfToday

object MaxGetter {
  def props(implicit cookie: Cookie): Props = Props(new MaxGetter)

  private[this] var id = -1
  private def getId: Int = {
    id += 1
    id
  }

  case class Request(requestId: Int)
  case class Success(requestId: Int, max: Int)
  case class Fail(requestId: Int)
}

class MaxGetter(implicit cookie: Cookie)
  extends Actor with ActorLogging {
  import MaxGetter._

  val id: Int = getId

  override def preStart(): Unit = {
    log.info("MaxGetter {} started", id)
  }

  override def postStop(): Unit = {
    log.info("MaxGetter {} stoped", id)
  }

  override def receive: Receive = {
    case Request(requestId) =>
      log.info("MaxGetter {} receives request:{}", id, requestId)
      getMaxOfToday match {
        case Some(max) =>
          log.info("MaxGetter {} succeeds for request:{}", id, requestId)
          sender() ! Success(requestId, max)
        case None =>
          log.info("MaxGetter {} fails for request:{}", id, requestId)
          sender() ! Fail(requestId)
      }
  }
}
