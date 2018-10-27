package services.xis.elastic

import scala.concurrent.duration.DurationInt
import scala.collection.mutable.{Map => MMap}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{TestProbe, TestKit}

import services.xis.elastic.actors._
import services.xis.crawl.ConnectUtil.Cookie
import services.xis.crawl.LoginUtil.login
import services.xis.crawl.ArticleSummary

class IndexerSpec extends TestKit(ActorSystem("IndexerSpec"))
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  private implicit val cookie: Cookie = MMap()

  private val host = "localhost"
  private val port0 = 9200
  private val port1 = 9300
  private val protocol = "http"
  private val index = "portal5"
  private val typ = "article"
  private val indexer = new Indexer(host, port0, port1, protocol, index, typ)

  private val requestId = 42
  private val max = 2329
  private val len = 15
  private val board = "student_notice"
  private val id = "11540544028732"
  private val title = "제4회 융합기초학부 이해를 위한 공개세미나 참석 신청 안내"
  private val link = "https://portal.kaist.ac.kr/board/fileMngr?cmd=down&boardId=student_notice&bltnNo=11540532763654&fileSeq=1&subId=sub06"
  private val fileLen = 5304222

  override def beforeAll: Unit = {
    login
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "MaxGetter should work" in {
    val probe = TestProbe()
    val ref = system.actorOf(MaxGetter.props)

    ref.tell(MaxGetter.Request(requestId), probe.ref)
    val response = probe.expectMsgType[MaxGetter.Success]
    response.requestId should ===(requestId)
    response.max should be >= max
  }

  "IndexGetter should work" in {
    val probe = TestProbe()
    val ref = system.actorOf(SummaryGetter.props)

    ref.tell(SummaryGetter.Request(requestId, 1), probe.ref)
    val response = probe.expectMsgType[SummaryGetter.Result]
    response.requestId should ===(requestId)
    response.page should ===(1)
    response.summaries should have length len
  }

  "ArticleGetter should work" in {
    val probe = TestProbe()
    val ref = system.actorOf(ArticleGetter.props)

    ref.tell(ArticleGetter.Request(requestId, board, id), probe.ref)
    val response = probe.expectMsgType[ArticleGetter.Success]
    response.requestId should ===(requestId)
    response.board should ===(board)
    response.id should ===(id)
    response.article.title should ===(title)
  }

  "FileGetter should work" in {
    val probe = TestProbe()
    val ref = system.actorOf(FileGetter.props)

    val meta = FileMeta("", link, true)
    ref.tell(FileGetter.Request(requestId, id, meta), probe.ref)
    val response = probe.expectMsgType[FileGetter.Result]
    response.requestId should ===(requestId)
    response.id should ===(id)
    response.meta should ===(meta)
    response.bytes should have length fileLen
  }

  "TextExtractor should work" in {
    val probe = TestProbe()
    val ref = system.actorOf(TextExtractor.props)

    val meta = FileMeta("a.docx", "", true)
    val bytes = Array[Byte]()
    ref.tell(TextExtractor.Request(requestId, id, meta, bytes), probe.ref)
    val response = probe.expectMsgType[TextExtractor.Result]
    response.requestId should ===(requestId)
    response.id should ===(id)
    response.meta should ===(meta)
    response.text should ===("")
  }

  "ElasticReader should work" in {
    val probe = TestProbe()
    val ref = system.actorOf(ElasticReader.props(indexer))

    val summary = ArticleSummary("", "1", 0)
    ref.tell(ElasticReader.Request(requestId, summary), probe.ref)
    val response = probe.expectMsgType[ElasticReader.Success]
    response.requestId should ===(requestId)
    response.summary should ===(summary)
    response.exist should ===(false)
  }
}
