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

  override def beforeAll: Unit = {
    login
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "MaxGetter should work" in {
    val probe = TestProbe()
    val maxGetter = system.actorOf(MaxGetter.props)

    maxGetter.tell(MaxGetter.Request(42), probe.ref)
    val response = probe.expectMsgType[MaxGetter.Success]
    response.requestId should ===(42)
    response.max should be >= 2329
  }
}
