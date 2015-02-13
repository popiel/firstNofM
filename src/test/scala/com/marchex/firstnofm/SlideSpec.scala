package com.marchex.firstnofm

import akka.actor.ActorSystem
import org.scalatest.{Matchers, FunSpec}
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Random, Try, Success, Failure}

class SlideSpec extends FunSpec with Matchers {
  import ExecutionContext.Implicits.global

  def standardTests(invoke: (Int, List[Future[String]]) => Future[List[Try[String]]]) {
    it("should return the first completed futures of a list of futures") {
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first3 = invoke(3, futures)
      val first4 = invoke(4, futures)

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(200); promises(x - 1).complete(Success("F" + x)) } } }.start

      first3 should not be ('completed)
      val r3 = Await.result(first3, 700 milliseconds)
      r3.map{_.get} should equal (List("F1", "F2", "F3"))

      first4 should not be ('completed)
      val r4 = Await.result(first4, 300 milliseconds)
      r4.map{_.get} should equal (List("F1", "F2", "F3", "F4"))

      futures.find(!_.isCompleted) should not be ('empty)
    }

    it("should not block forever when asking for more futures than exist") {
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first15 = invoke(15, futures)

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      val r15 = Await.result(first15, 200 milliseconds)
      r15.map{_.get} should equal (List("F1", "F2", "F3", "F4", "F5"))
    }
  }

  describe("SlideCode.firstNCompletedOfMInQuadraticSpace") {
    standardTests { (n, futures) => SlideCode.firstNCompletedOfMInQuadraticSpace(n, futures) }
  }

  describe("SlideCode.firstNCompletedOfMInQuadraticTime") {
    standardTests { (n, futures) => SlideCode.firstNCompletedOfMInQuadraticTime(n, futures) }
  }

  describe("SlideCode.firstNCompletedOfM") {
    standardTests { (n, futures) => SlideCode.firstNCompletedOfM(n, futures) }
  }

  describe("SlideActor.firstNCompletedOfM") {
    // Sloppy!  This ActorSystem is never shut down.
    implicit val system = ActorSystem("SlideActor-firstNCompletedOfM")
    standardTests { (n, futures) => SlideActor.firstNCompletedOfM(n, futures) }
  }

  describe("SlideCode.firstNSuccessfulOfM") {
    it("should return the first successful futures of a list of futures") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 9).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first3 = SlideCode.firstNSuccessfulOfM(3, futures)
      val first4 = SlideCode.firstNSuccessfulOfM(4, futures)

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run {
        (1 to 9) foreach { x =>
          Thread.sleep(200)
          promises(x - 1).complete(if (x % 2 == 1) Success("F" + x) else Failure(new IllegalStateException("Thbbt")))
        }
      }}.start

      first3 should not be ('completed)
      val r3 = Await.result(first3, 1100 milliseconds)
      r3 should equal (List("F1", "F3", "F5"))

      first4 should not be ('completed)
      val r4 = Await.result(first4, 500 milliseconds)
      r4 should equal (List("F1", "F3", "F5", "F7"))

      futures.find(!_.isCompleted) should not be ('empty)
    }

    it("should not block forever when asking for more futures than exist") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first15 = SlideCode.firstNSuccessfulOfM(15, futures)

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      val r15 = Await.result(first15, 200 milliseconds)
      r15 should equal (List("F1", "F2", "F3", "F4", "F5"))
    }

    it("should not block forever when asking for more futures than succeed") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first3 = SlideCode.firstNSuccessfulOfM(3, futures)

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run {
        (1 to 5) foreach { x =>
          Thread.sleep(20)
          promises(x - 1).complete(if (x == 4) Success("F" + x) else Failure(new IllegalStateException("Thbbt")))
        }
      }}.start

      val r3 = Await.result(first3, 200 milliseconds)
      r3 should equal (List("F4"))
    }
  }

  describe("SlideCode.firstNFilteredOfM") {
    it("should return the first filtered futures of a list of futures") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 9).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first3 = SlideCode.firstNFilteredOfM(3, futures){ s => s.get.tail.toInt % 2 == 1 }
      val first4 = SlideCode.firstNFilteredOfM(4, futures){ s => s.get.tail.toInt % 2 == 1 }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run {
        (1 to 9) foreach { x =>
          Thread.sleep(200)
          promises(x - 1).complete(Success("F" + x))
        }
      }}.start

      first3 should not be ('completed)
      val r3 = Await.result(first3, 1100 milliseconds)
      r3.map(_.get) should equal (List("F1", "F3", "F5"))

      first4 should not be ('completed)
      val r4 = Await.result(first4, 500 milliseconds)
      r4.map(_.get) should equal (List("F1", "F3", "F5", "F7"))

      futures.find(!_.isCompleted) should not be ('empty)
    }

    it("should not block forever when asking for more futures than exist") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first15 = SlideCode.firstNFilteredOfM(15, futures){ s => true }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      val r15 = Await.result(first15, 200 milliseconds)
      r15.map(_.get) should equal (List("F1", "F2", "F3", "F4", "F5"))
    }

    it("should not block forever when asking for more futures than succeed") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first3 = SlideCode.firstNFilteredOfM(3, futures){ s => s.get == "F4" }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      val r3 = Await.result(first3, 200 milliseconds)
      r3.map(_.get) should equal (List("F4"))
    }
  }

  describe("SlideActor.firstNFilteredOfM") {
    // Sloppy!  This ActorSystem is never shut down.
    implicit val system = ActorSystem("SlideActor-firstNFilteredOfM")

    it("should return the first filtered futures of a list of futures") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 9).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first3 = SlideActor.firstNFilteredOfM(3, futures){ s => s.get.tail.toInt % 2 == 1 }
      val first4 = SlideActor.firstNFilteredOfM(4, futures){ s => s.get.tail.toInt % 2 == 1 }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run {
        (1 to 9) foreach { x =>
          Thread.sleep(200)
          promises(x - 1).complete(Success("F" + x))
        }
      }}.start

      first3 should not be ('completed)
      val r3 = Await.result(first3, 1100 milliseconds)
      r3.map(_.get) should equal (List("F1", "F3", "F5"))

      first4 should not be ('completed)
      val r4 = Await.result(first4, 500 milliseconds)
      r4.map(_.get) should equal (List("F1", "F3", "F5", "F7"))

      futures.find(!_.isCompleted) should not be ('empty)
    }

    it("should not block forever when asking for more futures than exist") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first15 = SlideActor.firstNFilteredOfM(15, futures){ s => true }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      val r15 = Await.result(first15, 200 milliseconds)
      r15.map(_.get) should equal (List("F1", "F2", "F3", "F4", "F5"))
    }

    it("should not block forever when asking for more futures than succeed") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first3 = SlideActor.firstNFilteredOfM(3, futures){ s => s.get == "F4" }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      val r3 = Await.result(first3, 200 milliseconds)
      r3.map(_.get) should equal (List("F4"))
    }
  }
}
