package services.xis.elastic.actors

import java.io.IOException

import akka.actor.{Actor, ActorLogging, Props}

import services.xis.elastic.FileMeta
import services.xis.extractor.Extractor

object TextExtractor {
  def props: Props = Props(new TextExtractor)

  private[this] var id = -1
  private def getId: Int = {
    id += 1
    id
  }

  case class Request(requestId: Int, id: String, meta: FileMeta, bytes: Array[Byte])
  case class Result(requestId: Int, id: String, meta: FileMeta, text: String)
}

class TextExtractor extends Actor with ActorLogging {
  import TextExtractor._

  val id: Int = getId

  override def preStart(): Unit = {
    log.info("TextExtractor:{} starts", id)
  }

  override def postStop(): Unit = {
    log.info("TextExtractor:{} stops", id)
  }

  override def receive: Receive = {
    case Request(requestId, aid, meta, bytes) =>
      log.info("TextExtractor:{} receives request:{} for {}",
        id, requestId, aid)
      val text = Extractor.extract(meta.name, bytes)
      log.info("TextExtractor:{} obtains for {} of request:{}",
        id, aid, requestId)
      sender() ! Result(requestId, aid, meta, text)
  }
}
