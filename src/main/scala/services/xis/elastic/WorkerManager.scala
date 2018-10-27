package services.xis.elastic

import scala.collection.mutable.{Queue, Set => MSet}

import akka.actor.{Props, ActorRef, ActorContext}

class WorkerManager[T](implicit context: ActorContext, self: ActorRef) {
  private val idleQ: Queue[ActorRef] = Queue()
  private val workingSet: MSet[ActorRef] = MSet()
  private val pendingQ: Queue[T] = Queue()

  def init(num: Int, props: Props): Unit =
    for (i <- 1 to num) idleQ += context.actorOf(props)

  def pend(t: T): Unit = pendingQ += t
  def pend(t: Seq[T]): Unit = pendingQ ++= t

  def dealloc(ref: ActorRef): Unit = {
    workingSet -= ref
    idleQ += ref
  }

  def alloc(): Unit = {
    while (idleQ.nonEmpty && pendingQ.nonEmpty) {
      val ref = idleQ.dequeue
      workingSet += ref
      ref ! pendingQ.dequeue
    }
  }

  def isFinish: Boolean = workingSet.isEmpty && pendingQ.isEmpty
}
