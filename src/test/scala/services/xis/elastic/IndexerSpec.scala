package services.xis.elastic

import scala.concurrent.duration.DurationInt
import scala.collection.mutable.{Map => MMap}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import akka.actor.{ActorSystem, PoisonPill}
import akka.testkit.{TestProbe, TestKit}

import services.xis.elastic.actors._
import services.xis.crawl.ConnectUtil.Cookie
import services.xis.crawl.LoginUtil.login

class IndexerSpec extends TestKit(ActorSystem("IndexerSpec"))
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  private implicit val cookie: Cookie = MMap()

  private val requestId = 42
  private val max = 2329
  private val len = 15
  private val board = "student_notice"
  private val id = "11540544028732"
  private val title = "제4회 융합기초학부 이해를 위한 공개세미나 참석 신청 안내"

  override def beforeAll: Unit = {
    login
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "MaxGetter should work" in {
    val probe = TestProbe()
    val maxGetter = system.actorOf(MaxGetter.props)

    maxGetter.tell(MaxGetter.Request(requestId), probe.ref)
    val response = probe.expectMsgType[MaxGetter.Success]
    response.requestId should ===(requestId)
    response.max should be >= max
  }

  "IndexGetter should work" in {
    val probe = TestProbe()
    val maxGetter = system.actorOf(SummaryGetter.props)

    maxGetter.tell(SummaryGetter.Request(requestId, 1), probe.ref)
    val response = probe.expectMsgType[SummaryGetter.Result]
    response.requestId should ===(requestId)
    response.page should ===(1)
    response.summaries should have length len
  }

  "ArticleGetter should work" in {
    val probe = TestProbe()
    val maxGetter = system.actorOf(ArticleGetter.props)

    maxGetter.tell(ArticleGetter.Request(requestId, board, id), probe.ref)
    val response = probe.expectMsgType[ArticleGetter.Success]
    response.requestId should ===(requestId)
    response.board should ===(board)
    response.id should ===(id)
    response.article.title should ===(title)
  }
}
