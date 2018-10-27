package services.xis.elastic.actors

import scala.collection.mutable.{Queue, Map => MMap, Set => MSet}

import akka.actor.{Actor, ActorLogging, Props, ActorRef}

import services.xis.elastic.WorkerManager
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
  ): Props =
    Props(new CrawlManager(false, 0, maxWorkerNum, summaryWorkerNum, articleWorkerNum))//, fileWorkerNum))

  def debugProps(
    pseudoMax: Int,
    maxWorkerNum: Int,
    summaryWorkerNum: Int,
    articleWorkerNum: Int//,
//    fileWorkerNum: Int
  ): Props =
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
) extends Actor with ActorLogging {
  import CrawlManager._

  private val start = System.currentTimeMillis
  private implicit val cookie: Cookie = MMap()

  private val maxM = new WorkerManager[MaxGetter.Request]
  private val summaryM = new WorkerManager[SummaryGetter.Request]
  private val articleM = new WorkerManager[ArticleGetter.Request]

  private val managers = List(maxM, summaryM, articleM)

  override def preStart(): Unit = {
    login
    maxM.init(maxWorkerNum, MaxGetter.props)
    summaryM.init(summaryWorkerNum, SummaryGetter.props)
    articleM.init(articleWorkerNum, ArticleGetter.props)
    log.info("CrawlManager starts at {}", start)
  }

  override def postStop(): Unit = {
    val end = System.currentTimeMillis
    log.info("CrawlManager stops at {}", end)
    log.info("CrawlManager worked for {} ms", end - start)
  }

  private val _receive: Receive = {
    case Start =>
      maxM.pend(MaxGetter.Request(getRequestId))

    case MaxGetter.Success(_, max) =>
      maxM.dealloc(sender())
      summaryM.pend(
        (1 to (if (debug) pseudoMax else 100))
          .map(SummaryGetter.Request(getRequestId, _)))
    case MaxGetter.Failure(_) =>
      maxM.dealloc(sender())
      maxM.pend(MaxGetter.Request(getRequestId))

    case SummaryGetter.Result(_, _, summaries) =>
      summaryM.dealloc(sender())
      articleM.pend(
        summaries.map{
          case ArticleSummary(id, board, _) =>
            ArticleGetter.Request(getRequestId, id, board)
        })

    case ArticleGetter.Success(_, _, _, article) =>
      articleM.dealloc(sender())
      log.debug("Succ: {}", article.title)
    case ArticleGetter.Failure(_, board, id) =>
      articleM.dealloc(sender())
      articleM.pend(ArticleGetter.Request(getRequestId, board, id))
  }

  override def receive: Receive = _receive andThen { _ => manageWorkers() }

  private def manageWorkers(): Unit = {
    managers.foreach(_.alloc)
    if (managers.forall(_.isFinish))
      context.stop(self)
  }
}
