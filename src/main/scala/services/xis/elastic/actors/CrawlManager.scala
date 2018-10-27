package services.xis.elastic.actors

import scala.collection.mutable.{Queue, Set => MSet}

import akka.actor.{Actor, ActorLogging, Props, ActorRef}

import services.xis.crawl.ConnectUtil.Cookie
import services.xis.crawl.LoginUtil.login
import services.xis.crawl.CrawlUtil.getMaxOfToday
import services.xis.crawl.ArticleSummary

object CrawlManager {
  def props(
    maxWorkerNum: Int,
    summaryWorkerNum: Int//,
//    articleWorkerNum: Int,
//    fileWorkerNum: Int
  )(implicit cookie: Cookie): Props =
    Props(new CrawlManager(maxWorkerNum, summaryWorkerNum))//, articleWorkerNum, fileWorkerNum))

  private[this] var id = -1
  private def getRequestId: Int = {
    id += 1
    id
  }

  case object Start
}

class CrawlManager(
  maxWorkerNum: Int,
  summaryWorkerNum: Int//,
//  articleWorkerNum: Int,
//  fileWorkerNum: Int
)(implicit cookie: Cookie) extends Actor with ActorLogging {
  import CrawlManager._

  private val start = System.currentTimeMillis

  private val maxIdleQ: Queue[ActorRef] = Queue()
  private val maxWorkingSet: MSet[ActorRef] = MSet()
  private val maxPendingQ: Queue[MaxGetter.Request] = Queue()

  private val summaryIdleQ: Queue[ActorRef] = Queue()
  private val summaryWorkingSet: MSet[ActorRef] = MSet()
  private val summaryPendingQ: Queue[SummaryGetter.Request] = Queue()

  override def preStart(): Unit = {
    login
    for (i <- 1 to maxWorkerNum)
      maxIdleQ += context.actorOf(MaxGetter.props)
    for (i <- 1 to summaryWorkerNum)
      summaryIdleQ += context.actorOf(SummaryGetter.props)
    log.info("CrawlManager starts at {}", start)
  }

  override def postStop(): Unit = {
    val end = System.currentTimeMillis
    log.info("CrawlManager stops at {}", end)
    log.info("CrawlManager worked for {} ms", end - start)
  }

  override def receive: Receive = {
    case Start =>
      maxPendingQ += MaxGetter.Request(getRequestId)
      manageWorkers()

    case MaxGetter.Success(_, max) =>
      maxWorkingSet -= sender()
      maxIdleQ += sender()
      summaryPendingQ ++=
//        (1 to max).map(SummaryGetter.Request(getRequestId, _))
        (1 to 100).map(SummaryGetter.Request(getRequestId, _))
      manageWorkers()
    case MaxGetter.Fail(_) =>
      maxWorkingSet -= sender()
      maxIdleQ += sender()
      maxPendingQ += MaxGetter.Request(getRequestId)
      manageWorkers()

    case SummaryGetter.Result(_, _, summaries) =>
      summaryWorkingSet -= sender()
      summaryIdleQ += sender()
      log.info("Result: {}", summaries.map(_.id).mkString(" "))
      manageWorkers()
  }

  private def manageWorkers(): Unit = {
    def _manage[T](
      idleQ: Queue[ActorRef], workingSet: MSet[ActorRef], pendingQ: Queue[T]
    ): Unit = {
      while (idleQ.nonEmpty && pendingQ.nonEmpty) {
        val ref = idleQ.dequeue
        workingSet += ref
        ref ! pendingQ.dequeue
      }
    }

    _manage(maxIdleQ, maxWorkingSet, maxPendingQ)
    _manage(summaryIdleQ, summaryWorkingSet, summaryPendingQ)

    if (List(
      maxWorkingSet, maxPendingQ,
      summaryWorkingSet, summaryPendingQ,
    ).forall(_.isEmpty))
      context.stop(self)
  }
}
