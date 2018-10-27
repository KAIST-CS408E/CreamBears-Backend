package services.xis.elastic.actors

import akka.actor.{Actor, ActorLogging, Props}

import services.xis.crawl.ConnectUtil.Cookie
import services.xis.crawl.CrawlUtil.getSummariesFromToday
import services.xis.crawl.ArticleSummary

object SummaryGetter {
  def props(implicit cookie: Cookie): Props = Props(new SummaryGetter)

  private[this] var id = -1
  private def getId: Int = {
    id += 1
    id
  }

  case class Request(requestId: Int, page: Int)
  case class Result(requestId: Int, page: Int, summaries: List[ArticleSummary])
}

class SummaryGetter(implicit cookie: Cookie) extends Actor with ActorLogging {
  import SummaryGetter._

  val id: Int = getId

  override def preStart(): Unit = {
    log.info("SummaryGetter:{} starts", id)
  }

  override def postStop(): Unit = {
    log.info("SummaryGetter:{} stops", id)
  }

  override def receive: Receive = {
    case Request(requestId, page) =>
      log.info("SummaryGetter:{} receives request:{} for page {}",
        id, requestId, page)
      val summaries = getSummariesFromToday(page)
      log.info("SummaryGetter:{} obtains result for page {} of request:{}",
			  id, page, requestId)
      sender() ! Result(requestId, page, summaries)
  }
}
