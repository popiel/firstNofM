package com.marchex.firstnofm

import akka.actor._
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent._
import scala.collection.mutable.ListBuffer
import scala.util.{Try,Success,Failure}

object SlideCode {
  def firstNCompletedOfMInQuadraticSpace[T](
    countN: Int, futures: List[Future[T]]
  )(
    implicit executor: ExecutionContext
  ): Future[List[Try[T]]] = {
    def putFirstInList(countN: Int, remaining: List[Future[T]],
                finished: ListBuffer[Try[T]]): Future[List[Try[T]]] = {
      if (countN < 1 || remaining.isEmpty) {
        Future.successful(finished.toList)
      } else {
        val markers = remaining.map(f => f.map{_ => f}.recover{case _ => f})
        Future.firstCompletedOf(markers).flatMap { first =>
          putFirstInList(countN - 1, remaining.filter(_ != first),
                         finished += first.value.get)
        }
      }
    }
    putFirstInList(countN, futures, new ListBuffer[Try[T]])
  }

  def firstNCompletedOfMInQuadraticTime[T](
    countN: Int, futures: List[Future[T]]
  )(
    implicit executor: ExecutionContext
  ): Future[List[Try[T]]] = {
    val actual = countN min futures.size
    val promises = Vector.fill(actual)(Promise[Try[T]]())
    val last = promises.last

    val handler: Try[T] => Unit = { result =>
      if (!last.isCompleted)
        promises.find(_ trySuccess result)
    }
    futures foreach { _ onComplete handler }
    Future.sequence(promises.toList map { _.future })
  }

  def firstNCompletedOfM[T](
    countN: Int, futures: List[Future[T]]
  )(
    implicit executor: ExecutionContext
  ): Future[List[Try[T]]] = {
    val actual = countN min futures.size
    val promises = Vector.fill(actual)(Promise[Try[T]]())
    val last = promises.last
    val completed = new AtomicInteger(0)
    val handler: Try[T] => Unit = { result =>
      if (!last.isCompleted)
        promises(completed.getAndIncrement) success result
    }
    futures foreach { _ onComplete handler }
    Future.sequence(promises.toList map { _.future })
  }

  def firstNSuccessfulOfM[T](countN: Int, futures: List[Future[T]])(


    implicit executor: ExecutionContext
  ): Future[List[T]] = {
    val total = futures.size
    val promises = Vector.fill(countN min total)(Promise[Try[T]]())
    val enough = Future.sequence(promises map { _.future })
    val successes = new AtomicInteger(0)
    val failures = new AtomicInteger(0)
    val handler: Try[T] => Unit = { result =>
      if (!enough.isCompleted) {
        if (result.isSuccess) {
          promises(successes.getAndIncrement).success(result)
        } else {
          val index = total - failures.incrementAndGet
          if (index < countN) promises(index).success(result)
    } } }
    futures foreach { _ onComplete handler }
    enough map { _.toList.filter(_.isSuccess).map(_.get) }
  }

  def firstNFilteredOfM[T](countN: Int, futures: List[Future[T]])(
    predicate: Try[T] => Boolean
  )(
    implicit executor: ExecutionContext
  ): Future[List[Try[T]]] = {
    val total = futures.size
    val promises = Vector.fill(countN min total)(Promise[Try[T]]())
    val enough = Future.sequence(promises map { _.future })
    val successes = new AtomicInteger(0)
    val failures = new AtomicInteger(0)
    val handler: Try[T] => Unit = { result =>
      if (!enough.isCompleted) {
        if (predicate(result)) {
          promises(successes.getAndIncrement).success(result)
        } else {
          val index = total - failures.incrementAndGet
          if (index < countN) promises(index).success(result)
    } } }
    futures foreach { _ onComplete handler }
    enough map { _.toList.filter(predicate) }
  }
}

object SlideActor {
  import ExecutionContext.Implicits.global

  class FirstNofMActor[T](limit: Int, done: Promise[List[Try[T]]]) extends Actor {
    val completed = ListBuffer[Try[T]]()
    var count = 0

    def receive = {
      case x: Try[T] =>
        completed += x
        count += 1
        if (count == limit) done.success(completed.toList)
    }
  }

  def firstNCompletedOfM[T](countN: Int, futures: List[Future[T]])
      (implicit factory: ActorRefFactory): Future[List[Try[T]]] = {
    val promise = Promise[List[Try[T]]]()
    val ref = factory.actorOf(Props(new FirstNofMActor(countN min futures.size, promise)))
    val handler: Try[T] => Unit = { ref ! _ }
    futures foreach { _ onComplete handler }
    promise.future onComplete { case _ => factory stop ref }
    promise.future
  }

  class FirstNFilteredofMActor[T](
      n: Int, m: Int, predicate: Try[T] => Boolean,
      done: Promise[List[Try[T]]]) extends Actor {
    val completed = ListBuffer[Try[T]]()
    var count = 0
    var all = 0

    def receive = {
      case x: Try[T] =>
        if (predicate(x)) {
          completed += x
          count += 1
        }
        all += 1
        if (count == n || all == m)
          done.success(completed.toList)
    }
  }

  def firstNFilteredOfM[T](
    countN: Int, futures: List[Future[T]]
  )(predicate: Try[T] => Boolean
  )(implicit factory: ActorRefFactory
  ): Future[List[Try[T]]] = {
    val promise = Promise[List[Try[T]]]()
    val ref = factory.actorOf(
      Props(new FirstNFilteredofMActor(
        countN min futures.size, futures.size,
        predicate, promise)))

    val handler: Try[T] => Unit = { ref ! _ }
    futures foreach { _ onComplete handler }
    promise.future onComplete { case _ => factory stop ref }
    promise.future
  }
}
