package services.xis.elastic.actors

import scala.collection.mutable.{Queue, Set => MSet}

import akka.actor.{Actor, ActorLogging, Props, ActorRef}

import services.xis.crawl.ConnectUtil.Cookie
import services.xis.crawl.LoginUtil.login
import services.xis.crawl.CrawlUtil.getMaxOfToday
import services.xis.crawl.ArticleSummary

object CrawlManager {
  def props(
    maxWorkerNum: Int//,
//    summaryWorkerNum: Int,
//    articleWorkerNum: Int,
//    fileWorkerNum: Int
  )(implicit cookie: Cookie): Props =
    Props(new CrawlManager(maxWorkerNum))//, summaryWorkerNum, articleWorkerNum, fileWorkerNum))

  private[this] var id = -1
  private def getRequestId: Int = {
    id += 1
    id
  }

  case object Start
}

class CrawlManager(
  maxWorkerNum: Int//,
//  summaryWorkerNum: Int,
//  articleWorkerNum: Int,
//  fileWorkerNum: Int
)(implicit cookie: Cookie) extends Actor with ActorLogging {
  import CrawlManager._

  private val maxIdleQ: Queue[ActorRef] = Queue()
  private val maxWorkingSet: MSet[ActorRef] = MSet()
  private val maxPendingReq: Queue[MaxGetter.Request] = Queue()

//  val summaryWorkers: ListBuffer[ActorRef] = ListBuffer()

  override def preStart(): Unit = {
    login
    for (i <- 1 to maxWorkerNum)
      maxIdleQ += context.actorOf(MaxGetter.props)
//    for (i <- 1 to summaryWorkerNum)
//      summaryWorkers += context.actorOf(SummaryGetter.props)
    log.info("CrawlManager started")
  }

  override def postStop(): Unit = {
    log.info("CrawlManager stoped")
  }

  override def receive: Receive = {
    case Start =>
      maxPendingReq += MaxGetter.Request(getRequestId)
      manageWorkers()
    case MaxGetter.Success(requestId, max) =>
      maxWorkingSet -= sender()
      maxIdleQ += sender()
      log.info("Succ: {}", max)
      manageWorkers()
    case MaxGetter.Fail(_) =>
      maxWorkingSet -= sender()
      maxIdleQ += sender()
      maxPendingReq += MaxGetter.Request(getRequestId)
      manageWorkers()
  }

  private def manageWorkers(): Unit = {
    while (maxIdleQ.nonEmpty && maxPendingReq.nonEmpty) {
      val ref = maxIdleQ.dequeue
      maxWorkingSet += ref
      ref ! maxPendingReq.dequeue
    }

    if (List(maxWorkingSet, maxPendingReq).forall(_.isEmpty))
      context.stop(self)
  }
}
