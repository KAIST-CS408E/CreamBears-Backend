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
    summaryWorkerNum: Int,
    articleWorkerNum: Int//,
//    fileWorkerNum: Int
  )(implicit cookie: Cookie): Props =
    Props(new CrawlManager(false, 0, maxWorkerNum, summaryWorkerNum, articleWorkerNum))//, fileWorkerNum))

  def debugProps(
    pseudoMax: Int,
    maxWorkerNum: Int,
    summaryWorkerNum: Int,
    articleWorkerNum: Int//,
//    fileWorkerNum: Int
  )(implicit cookie: Cookie): Props =
    Props(new CrawlManager(true, pseudoMax, maxWorkerNum, summaryWorkerNum, articleWorkerNum))//, fileWorkerNum))

  private[this] var id = -1
  private def getRequestId: Int = {
    id += 1
    id
  }

  case object Start
}

class CrawlManager(
  debug: Boolean,
  pseudoMax: Int,
  maxWorkerNum: Int,
  summaryWorkerNum: Int,
  articleWorkerNum: Int//,
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

  private val articleIdleQ: Queue[ActorRef] = Queue()
  private val articleWorkingSet: MSet[ActorRef] = MSet()
  private val articlePendingQ: Queue[ArticleGetter.Request] = Queue()

  override def preStart(): Unit = {
    login
    for (i <- 1 to maxWorkerNum)
      maxIdleQ += context.actorOf(MaxGetter.props)
    for (i <- 1 to summaryWorkerNum)
      summaryIdleQ += context.actorOf(SummaryGetter.props)
    for (i <- 1 to articleWorkerNum)
      articleIdleQ += context.actorOf(ArticleGetter.props)
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
        (1 to (if (debug) pseudoMax else 100))
          .map(SummaryGetter.Request(getRequestId, _))
      manageWorkers()
    case MaxGetter.Failure(_) =>
      maxWorkingSet -= sender()
      maxIdleQ += sender()
      maxPendingQ += MaxGetter.Request(getRequestId)
      manageWorkers()

    case SummaryGetter.Result(_, _, summaries) =>
      summaryWorkingSet -= sender()
      summaryIdleQ += sender()
      articlePendingQ ++=
        summaries.map{
          case ArticleSummary(id, board, _) =>
            ArticleGetter.Request(getRequestId, id, board)
        }
      manageWorkers()

    case ArticleGetter.Success(_, _, _, article) =>
      articleWorkingSet -= sender()
      articleIdleQ += sender()
      log.debug("Succ: {}", article.title)
      manageWorkers()
    case ArticleGetter.Failure(_, board, id) =>
      articleWorkingSet -= sender()
      articleIdleQ += sender()
      articlePendingQ += ArticleGetter.Request(getRequestId, board, id)
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
    _manage(articleIdleQ, articleWorkingSet, articlePendingQ)

    if (List(
      maxWorkingSet, maxPendingQ,
      summaryWorkingSet, summaryPendingQ,
      articleWorkingSet, articlePendingQ,
    ).forall(_.isEmpty))
      context.stop(self)
  }
}
