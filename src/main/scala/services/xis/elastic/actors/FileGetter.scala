package services.xis.elastic.actors

import java.io.IOException

import akka.actor.{Actor, ActorLogging, Props}

import services.xis.elastic.FileMeta
import services.xis.crawl.ConnectUtil.Cookie
import services.xis.crawl.CrawlUtil.getFile

object FileGetter {
  def props(implicit cookie: Cookie): Props = Props(new FileGetter)

  private[this] var id = -1
  private def getId: Int = {
    id += 1
    id
  }

  case class Request(requestId: Int, id: String, meta: FileMeta)
  case class Result(requestId: Int, id: String, meta: FileMeta, bytes: Array[Byte])
}

class FileGetter(implicit cookie: Cookie) extends Actor with ActorLogging {
  import FileGetter._

  val id: Int = getId

  override def preStart(): Unit = {
    log.info("FileGetter:{} starts", id)
  }

  override def postStop(): Unit = {
    log.info("FileGetter:{} stops", id)
  }

  override def receive: Receive = {
    case Request(requestId, aid, meta) =>
      log.info("FileGetter:{} receives request:{} for {}",
        id, requestId, aid)
      val bytes =
        try {
          getFile(meta.link)
        } catch {
          case _: IOException => Array[Byte]()
          case _: IllegalArgumentException => Array[Byte]()
        }
      log.info("FileGetter:{} obtains for {} of request:{}",
        id, aid, requestId)
      sender() ! Result(requestId, aid, meta, bytes)
  }
}
