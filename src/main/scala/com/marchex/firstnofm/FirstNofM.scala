package com.marchex.firstnofm

import scala.concurrent._
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable.Builder
import scala.util.{Try,Success,Failure}

object FirstNofM {
  // This produces a future that will never fail;
  // if it completes, it succeeds with the values of
  // the first N given futures to complete (successful or not).
  //
  // Because of the implementation of firstCompletedOf, this uses O(n*m) memory for onComplete handlers.
  // This memory usage could be avoided through the use of a synchronized mutable queue (or actor) to hold the results,
  // but that moves synchronization out of the futures and into the ancillary data structure,
  // which obviates the point of this exercise.
  //
  // Yes, I went slightly insane and decided to do this in the collection-type-preserving way...
  // that was as much an exercise as making the method only do O(n) builder appends.
  def firstNofBigMem[T, M[X] <: Traversable[X]](count: Int, futures: M[Future[T]])(implicit executor: ExecutionContext, cbf: CanBuildFrom[M[Future[T]], Try[T], M[Try[T]]]): Future[M[Try[T]]] = {
    val pairs = futures.map(f => (f, f.map{_ => f}.recover{case _ => f}))
    def helper(count: Int, marked: Traversable[(Future[T], Future[Future[T]])], finished: Builder[Try[T],M[Try[T]]]): Future[M[Try[T]]] = {
      if (count < 1 || marked.isEmpty) Future.successful(finished.result)
      else Future.firstCompletedOf(marked.map(_._2)).flatMap { first =>
        helper(count - 1, marked.filter(_._1 != first), finished += first.value.get)
      }
    }
    helper(count, pairs, cbf(futures))
  }

  // Sadly, this is the same or worse memory consumption as the above,
  // because the future mapping for markers uses onComplete,
  // and the onComplete handlers remain attached to the root futures.
  def firstNofNoPairs[T, M[X] <: Traversable[X]](count: Int, futures: M[Future[T]])(implicit executor: ExecutionContext, cbf: CanBuildFrom[M[Future[T]], Try[T], M[Try[T]]]): Future[M[Try[T]]] = {
    def helper(count: Int, remaining: Traversable[Future[T]], finished: Builder[Try[T],M[Try[T]]]): Future[M[Try[T]]] = {
      if (count < 1 || remaining.isEmpty) Future.successful(finished.result)
      else {
        val markers = remaining.map(f => f.map{_ => f}.recover{case _ => f})
        Future.firstCompletedOf(markers).flatMap { first =>
          helper(count - 1, remaining.filter(_ != first), finished += first.value.get)
        }
      }
    }
    helper(count, futures, cbf(futures))
  }

  // This was the first acceptable version.
  //
  // This produces a future that will never fail;
  // if it completes, it succeeds with the values of
  // the first N of M given futures to complete (successful or not).
  //
  // This version is O(n + m) memory and O(n^2 + m) cpu.
  // The cpu could be improved to O(n log n + m)
  // by binary search on which is the next promise to complete,
  // but if N is large enough that that matters, WTF are you doing?
  // I don't think we can do any better than that,
  // because we have to at least touch each of the M futures
  // to add the onComplete handler.
  //
  // Yes, I went a bit insane and decided to do this in the collection-type-preserving way...
  // supporting TraversableOnce instead of Traversable was an added challenge.
  def firstNofQuadratic[T, M[X] <: TraversableOnce[X]](count: Int, futures: M[Future[T]])(
    implicit executor: ExecutionContext,
    cbf1: CanBuildFrom[M[Future[T]], Future[Try[T]], M[Future[Try[T]]]],
    cbf2: CanBuildFrom[M[Future[Try[T]]], Try[T], M[Try[T]]]
  ): Future[M[Try[T]]] = {
    val promises = Array.fill(count)(Promise[Try[T]]())
    val last = promises.last
    val handler: Try[T] => Unit = { result => if (!last.isCompleted) promises.find(_ trySuccess result) }
    var actual = 0
    futures foreach { f => f onComplete handler; actual += 1 }
    val builder = cbf1(futures)
    (promises take actual) foreach { builder += _.future }
    Future.sequence(builder.result)
  }

  // This starts to violate the spirit of the challenge
  // of using only Futures for synchronization.
  // However, it still doesn't use Actors,
  // and is faster than pure-Future versions.
  //
  // This produces a future that will never fail;
  // if it completes, it succeeds with the values of
  // the first N of M given futures to complete (successful or not).
  //
  // This version is O(n + m) memory and O(n + m) cpu.
  // I don't think we can do any better than that,
  // because we have to at least touch each of the M futures
  // to add the onComplete handler.
  //
  // Yes, I went a bit insane and decided to do this in the collection-type-preserving way...
  // supporting TraversableOnce instead of Traversable was an added challenge.
  def firstNCompletedOf[T, M[X] <: TraversableOnce[X]](count: Int, futures: M[Future[T]])(
    implicit executor: ExecutionContext,
    cbf1: CanBuildFrom[M[Future[T]], Future[Try[T]], M[Future[Try[T]]]],
    cbf2: CanBuildFrom[M[Future[Try[T]]], Try[T], M[Try[T]]]
  ): Future[M[Try[T]]] = {
    val promises = Array.fill(count)(Promise[Try[T]]())
    val last = promises.last
    val completed = new java.util.concurrent.atomic.AtomicInteger(0)
    val handler: Try[T] => Unit = { result =>
      if (!last.isCompleted) {
        val position = completed.getAndIncrement
        if (position < count) promises(position) success result
      }
    }
    var actual = 0
    futures foreach { f => f onComplete handler; actual += 1 }
    val builder = cbf1(futures)
    builder.sizeHint(promises.size min actual)
    (promises take actual) foreach { builder += _.future }
    Future.sequence(builder.result)
  }


  // This produces a future that will never fail;
  // if it completes, it succeeds with the contents of
  // the first N of M given futures to succeed
  // (or all the successful futures, if less than N).
  //
  // This version is O(n + m) memory and O(n + m) cpu.
  //
  // This requires Traversable instead of TraversableOnce
  // because we need to know the size of the set of futures
  // before we add the onComplete handlers.
  def firstNSuccessfulOf[T, M[X] <: Traversable[X]](count: Int, futures: M[Future[T]])(
    implicit executor: ExecutionContext,
    cbf: CanBuildFrom[M[Future[T]], T, M[T]]
  ): Future[M[T]] = {
    val total = futures.size
    val promises = Vector.fill(count min total)(Promise[Try[T]]())
    val enough = Future.sequence(promises map { _.future })
    val successes = new java.util.concurrent.atomic.AtomicInteger(0)
    val failures = new java.util.concurrent.atomic.AtomicInteger(0)
    val handler: Try[T] => Unit = { result =>
      if (!enough.isCompleted) {
        if (result.isSuccess) {
          val position = successes.getAndIncrement
          if (position < promises.size) promises(position).success(result)
        } else {
          val index = total - failures.incrementAndGet
          if (index < count) promises(index).success(result)
        }
      }
    }
    futures foreach { _ onComplete handler }
    enough map { results =>
      val builder = cbf(futures)
      builder.sizeHint(count min total)
      results foreach { case Success(s) => builder += s; case _ => () }
      builder.result
    }
  }

  // This produces a future that will never fail;
  // if it completes, it succeeds with the contents of
  // the first N of M given futures to pass the given predicate
  // (or all the passing futures, if less than N).
  //
  // This version is O(n + m) memory and O(n + m) cpu.
  //
  // This requires Traversable instead of TraversableOnce
  // because we need to know the size of the set of futures
  // before we add the onComplete handlers.
  def firstNFilteredOf[T, M[X] <: Traversable[X]](count: Int, futures: M[Future[T]])(
    predicate: Try[T] => Boolean
  )(
    implicit executor: ExecutionContext,
    cbf: CanBuildFrom[M[Future[T]], Try[T], M[Try[T]]]
  ): Future[M[Try[T]]] = {
    val total = futures.size
    val promises = Vector.fill(count min total)(Promise[Try[T]]())
    val enough = Future.sequence(promises map { _.future })
    val successes = new java.util.concurrent.atomic.AtomicInteger(0)
    val failures = new java.util.concurrent.atomic.AtomicInteger(0)
    val handler: Try[T] => Unit = { result =>
      if (!enough.isCompleted) {
        if (predicate(result)) {
          val position = successes.getAndIncrement
          if (position < promises.size) promises(position).success(result)
        } else {
          val index = total - failures.incrementAndGet
          if (index < count) promises(index).success(result)
        }
      }
    }
    futures foreach { _ onComplete handler }
    enough map { results =>
      val builder = cbf(futures)
      builder.sizeHint(count min total)
      results foreach { result => if (predicate(result)) builder += result }
      builder.result
    }
  }

  // This produces a future that succeeds with the first non-None result
  // of the provided selector, or fails after all provided futures complete.
  // The selector is called each time one of the provided futures completes,
  // and is passed the values of all futures that have completed to that point.
  //
  // This O(n) memory and O(n^2) cpu.
  //
  // This requires Traversable instead of TraversableOnce
  // because we need to know the size of the set of futures
  // before we add the onComplete handlers.
  def firstSelectedOf[T, R](futures: Traversable[Future[T]])(
    selector: Vector[Try[T]] => Option[R]
  )(
    implicit executor: ExecutionContext
  ): Future[R] = {
    val total = futures.size
    val promises = Vector.fill(total)(Promise[T]())
    val answer = Promise[R]()
    val completed = new java.util.concurrent.atomic.AtomicInteger(0)
    val handler: Try[T] => Unit = { result =>
      if (!answer.isCompleted) {
        val index = completed.getAndIncrement
        promises(index).complete(result)
        selector(promises.take(index+1).filter(_.isCompleted).map(_.future.value.get)) foreach { value =>
          answer.trySuccess(value)
        }
        if (index == total - 1) answer.tryFailure(new NoSuchElementException)
      }
    }
    futures foreach { _ onComplete handler }
    answer.future
  }
}
