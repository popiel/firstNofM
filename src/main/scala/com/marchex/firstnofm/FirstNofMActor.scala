package com.marchex.firstnofm

import akka.actor._
import scala.concurrent._
import scala.collection.mutable.ListBuffer
import scala.util.{Try,Success,Failure}

object FirstNofMActor {
  class FirstNCompletedActor[T](limit: Int, futures: List[Future[T]], done: Promise[List[Try[T]]]) extends Actor {
    val completed = ListBuffer[Try[T]]()
    var count = 0

    override def preStart() {
      super.preStart()
      implicit val ec = context.dispatcher
      val handler: Try[T] => Unit = { self ! _ }
      futures foreach { _ onComplete handler }
      done.future onComplete { case _ => context stop self }
    }

    def receive = {
      case x: Try[T] =>
        completed += x
        count += 1
        if (count == limit) done.success(completed.toList)
    }
  }

  def firstNCompletedOf[T](count: Int, futures: List[Future[T]])(implicit factory: ActorRefFactory): Future[List[Try[T]]] = {
    val promise = Promise[List[Try[T]]]()
    factory.actorOf(Props(new FirstNCompletedActor(count min futures.size, futures, promise)))
    promise.future
  }
}
