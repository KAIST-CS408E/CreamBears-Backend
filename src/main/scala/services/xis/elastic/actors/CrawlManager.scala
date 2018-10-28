package services.xis.elastic.actors

import scala.collection.mutable.{Queue, Map => MMap, Set => MSet}

import akka.actor.{Actor, ActorLogging, Props, ActorRef}

import services.xis.elastic.{Indexer, WorkerManager, ArticleDocument}
import services.xis.crawl.ConnectUtil.Cookie
import services.xis.crawl.LoginUtil.login
import services.xis.crawl.CrawlUtil.getMaxOfToday
import services.xis.crawl.ArticleSummary

object CrawlManager {
  def props(
    indexer: Indexer,
    maxWorkerNum: Int,
    summaryWorkerNum: Int,
    articleWorkerNum: Int,
    fileWorkerNum: Int,
    extractWorkerNum: Int,
    readWorkerNum: Int,
    writeWorkerNum: Int
  ): Props =
    Props(new CrawlManager(
      false, 0, 0, indexer,
      maxWorkerNum, summaryWorkerNum, articleWorkerNum,
      fileWorkerNum, extractWorkerNum, readWorkerNum, writeWorkerNum
    ))

  def debugProps(
    start: Int,
    end: Int,
    indexer: Indexer,
    maxWorkerNum: Int,
    summaryWorkerNum: Int,
    articleWorkerNum: Int,
    fileWorkerNum: Int,
    extractWorkerNum: Int,
    readWorkerNum: Int,
    writeWorkerNum: Int
  ): Props =
    Props(new CrawlManager(
      true, start, end, indexer,
      maxWorkerNum, summaryWorkerNum, articleWorkerNum,
      fileWorkerNum, extractWorkerNum, readWorkerNum, writeWorkerNum
    ))

  private[this] var id = -1
  private def getRequestId: Int = {
    id += 1
    id
  }

  case object Start
  case object Info
}

class CrawlManager(
  debug: Boolean,
  start: Int,
  end: Int,
  indexer: Indexer,
  maxWorkerNum: Int,
  summaryWorkerNum: Int,
  articleWorkerNum: Int,
  fileWorkerNum: Int,
  extractWorkerNum: Int,
  readWorkerNum: Int,
  writeWorkerNum: Int
) extends Actor with ActorLogging {
  import CrawlManager._

  private val startTime = System.currentTimeMillis
  private implicit val cookie: Cookie = MMap()

  private val maxM = new WorkerManager[MaxGetter.Request]
  private val summaryM = new WorkerManager[SummaryGetter.Request]
  private val articleM = new WorkerManager[ArticleGetter.Request]
  private val fileM = new WorkerManager[FileGetter.Request]
  private val extractM = new WorkerManager[TextExtractor.Request]
  private val readM = new WorkerManager[ElasticReader.Request]
  private val writeM = new WorkerManager[ElasticWriter.Request]

  private val managers =
    List(maxM, summaryM, articleM, fileM, extractM, readM, writeM)

  private val articles = MMap[String, ArticleDocument]()

  override def preStart(): Unit = {
    login
    maxM.init(maxWorkerNum, MaxGetter.props)
    summaryM.init(summaryWorkerNum, SummaryGetter.props)
    articleM.init(articleWorkerNum, ArticleGetter.props)
    fileM.init(fileWorkerNum, FileGetter.props)
    extractM.init(extractWorkerNum, TextExtractor.props)
    readM.init(readWorkerNum, ElasticReader.props(indexer))
    writeM.init(writeWorkerNum, ElasticWriter.props(indexer))
    log.info("CrawlManager starts at {}", startTime)
  }

  override def postStop(): Unit = {
    val endTime = System.currentTimeMillis
    log.info("CrawlManager stops at {}", endTime)
    log.info("CrawlManager worked for {} ms", endTime - startTime)
  }

  private val _receive: Receive = {
    case Start =>
      maxM.pend(MaxGetter.Request(getRequestId))
    case Info =>
      log.info("{}", managers.map(m => (m.workingSize, m.pendingSize)).mkString("\n"))

    case MaxGetter.Success(_, max) =>
      maxM.dealloc(sender())
      val pages = (1 to max).filter(p => !debug || (start <= p && p <= end))
      summaryM.pend(pages.map(SummaryGetter.Request(getRequestId, _)))
    case MaxGetter.Failure(_) =>
      maxM.dealloc(sender())
      maxM.pend(MaxGetter.Request(getRequestId))

    case SummaryGetter.Result(_, _, summaries) =>
      summaryM.dealloc(sender())
      readM.pend(summaries.map(ElasticReader.Request(getRequestId, _)))

    case ArticleGetter.Success(_, _, id, article) =>
      articleM.dealloc(sender())
      val artDoc = new ArticleDocument(article)
      if (artDoc.complete)
        writeM.pend(ElasticWriter.CreateRequest(getRequestId, artDoc))
      else {
        articles += (id -> artDoc)
        fileM.pend(artDoc.files.map(FileGetter.Request(getRequestId, id, _)))
      }
    case ArticleGetter.Failure(_, board, id) =>
      articleM.dealloc(sender())
      articleM.pend(ArticleGetter.Request(getRequestId, board, id))

    case FileGetter.Result(_, aid, meta, bytes) =>
      fileM.dealloc(sender())
      extractM.pend(TextExtractor.Request(getRequestId, aid, meta, bytes))

    case TextExtractor.Result(_, aid, meta, text) =>
      extractM.dealloc(sender())
      val artDoc = articles(aid)
      artDoc.add(meta, text)
      if (artDoc.complete) {
        articles -= aid
        writeM.pend(ElasticWriter.CreateRequest(getRequestId, artDoc))
      }

    case ElasticReader.Success(_, summ, exist) =>
      readM.dealloc(sender())
      if (exist)
        writeM.pend(ElasticWriter.UpdateRequest(getRequestId, summ))
      else
        articleM.pend(ArticleGetter.Request(getRequestId, summ.board, summ.id))
    case ElasticReader.Failure(_, summ) =>
      readM.dealloc(sender())
      readM.pend(ElasticReader.Request(getRequestId, summ))

    case ElasticWriter.Success(_) =>
      writeM.dealloc(sender())
    case ElasticWriter.CreateFailure(_, artDoc) =>
      writeM.dealloc(sender())
      writeM.pend(ElasticWriter.CreateRequest(getRequestId, artDoc))
    case ElasticWriter.UpdateFailure(_, summ) =>
      writeM.dealloc(sender())
      writeM.pend(ElasticWriter.UpdateRequest(getRequestId, summ))
  }

  override def receive: Receive = _receive andThen { _ => manageWorkers() }

  private def manageWorkers(): Unit = {
    managers.foreach(_.alloc)
    if (managers.forall(_.isFinish))
      context.stop(self)
  }
}
