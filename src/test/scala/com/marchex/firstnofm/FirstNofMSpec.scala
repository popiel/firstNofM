package com.marchex.firstnofm

import akka.actor.ActorSystem
import org.scalatest.{Matchers, FunSpec}
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Random, Try, Success, Failure}

class FirstNofMSpec extends FunSpec with Matchers {
  import ExecutionContext.Implicits.global

  def standardTestsTO(invoke: (Int, TraversableOnce[Future[String]]) => Future[TraversableOnce[Try[String]]]) {
    it("should return the first completed futures of a list of futures") {
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first3 = invoke(3, futures.iterator)
      val first4 = invoke(4, futures)

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(200); promises(x - 1).complete(Success("F" + x)) } } }.start

      first3 should not be ('completed)
      val r3 = Await.result(first3, 700 milliseconds)
      r3.map{_.get}.toList should equal (List("F1", "F2", "F3"))

      first4 should not be ('completed)
      val r4 = Await.result(first4, 300 milliseconds)
      r4.map{_.get}.toList should equal (List("F1", "F2", "F3", "F4"))

      futures.find(!_.isCompleted) should not be ('empty)
    }

    it("should not block forever when asking for more futures than exist") {
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future)).toList
      
      val first15 = invoke(15, futures)

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      val r15 = Await.result(first15, 200 milliseconds)
      r15.map{_.get}.toList should equal (List("F1", "F2", "F3", "F4", "F5"))
    }
  }

  def standardTestsT(invoke: (Int, List[Future[String]]) => Future[List[Try[String]]]) {
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

  describe("FirstNofM.firstNofBigMem") {
    standardTestsT { (n, futures) => FirstNofM.firstNofBigMem(n, futures) }
  }

  describe("FirstNofM.firstNofQuadratic") {
    standardTestsTO { (n, futures) => FirstNofM.firstNofQuadratic(n, futures) }
  }

  describe("FirstNofM.firstNCompletedOf") {
    standardTestsTO { (n, futures) => FirstNofM.firstNCompletedOf(n, futures) }
  }

  describe("FirstNofMActor.firstNCompletedOf") {
    // Sloppy!  This ActorSystem is never shut down.
    implicit val system = ActorSystem("Foo")
    standardTestsT { (n, futures) => FirstNofMActor.firstNCompletedOf(n, futures) }
  }

  describe("FirstNofM.firstNSuccessfulOf") {
    it("should return the first successful futures of a list of futures") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 9).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future))
      
      val first3 = FirstNofM.firstNSuccessfulOf(3, futures)
      val first4 = FirstNofM.firstNSuccessfulOf(4, futures)

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
      val futures = Random.shuffle(promises.map(_.future))
      
      val first15 = FirstNofM.firstNSuccessfulOf(15, futures)

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      val r15 = Await.result(first15, 200 milliseconds)
      r15 should equal (List("F1", "F2", "F3", "F4", "F5"))
    }

    it("should not block forever when asking for more futures than succeed") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future))
      
      val first3 = FirstNofM.firstNSuccessfulOf(3, futures)

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

  describe("FirstNofM.firstNFilteredOf") {
    it("should return the first filtered futures of a list of futures") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 9).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future))
      
      val first3 = FirstNofM.firstNFilteredOf(3, futures){ s => s.get.tail.toInt % 2 == 1 }
      val first4 = FirstNofM.firstNFilteredOf(4, futures){ s => s.get.tail.toInt % 2 == 1 }

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
      val futures = Random.shuffle(promises.map(_.future))
      
      val first15 = FirstNofM.firstNFilteredOf(15, futures){ s => true }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      val r15 = Await.result(first15, 200 milliseconds)
      r15.map(_.get) should equal (List("F1", "F2", "F3", "F4", "F5"))
    }

    it("should not block forever when asking for more futures than succeed") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future))
      
      val first3 = FirstNofM.firstNFilteredOf(3, futures){ s => s.get == "F4" }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      val r3 = Await.result(first3, 200 milliseconds)
      r3.map(_.get) should equal (List("F4"))
    }
  }

  describe("FirstNofM.firstSelectedOf") {
    it("should return the first filtered futures of a list of futures") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 9).map(_ => Promise[Int]())
      val futures = Random.shuffle(promises.map(_.future))
      
      val first3 = FirstNofM.firstSelectedOf(futures){ vals => val r = vals.filter(_.get % 2 == 1); if (r.size == 3) Some(r) else None }
      val first4 = FirstNofM.firstSelectedOf(futures){ vals => val r = vals.filter(_.get % 2 == 1); if (r.size == 4) Some(r) else None }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run {
        (1 to 9) foreach { x =>
          Thread.sleep(200)
          promises(x - 1).complete(Success(x))
        }
      }}.start

      first3 should not be ('completed)
      val r3 = Await.result(first3, 1100 milliseconds)
      r3.map(_.get) should equal (List(1, 3, 5))

      first4 should not be ('completed)
      val r4 = Await.result(first4, 500 milliseconds)
      r4.map(_.get) should equal (List(1, 3, 5, 7))

      futures.find(!_.isCompleted) should not be ('empty)
    }

    it("should not block forever when the selector never returns Some") {
      import ExecutionContext.Implicits.global
      val promises = (1 to 5).map(_ => Promise[String]())
      val futures = Random.shuffle(promises.map(_.future))
      
      val first15 = FirstNofM.firstSelectedOf(futures){ s => None }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run { (1 to 5) foreach { x => Thread.sleep(20); promises(x - 1).complete(Success("F" + x)) } } }.start

      a[NoSuchElementException] shouldBe thrownBy { Await.result(first15, 200 milliseconds) }
    }

    it("should provide the result of the selector, whatever type is within the Some") {
      val promises = (1 to 9).map(_ => Promise[Int]())
      val futures = Random.shuffle(promises.map(_.future))
      
      val first3 = FirstNofM.firstSelectedOf(futures){ vals => val r = vals.filter(_.get % 2 == 1); if (r.size == 3) Some(r) else None }
      val first4 = FirstNofM.firstSelectedOf(futures){ vals => val r = vals.filter(_.get % 2 == 1); if (r.size == 4) Some("hooray") else None }

      // Do this in a gross way to avoid consuming an entire thread pool
      new Thread { override def run {
        (1 to 9) foreach { x =>
          Thread.sleep(200)
          promises(x - 1).complete(Success(x))
        }
      }}.start

      first3 should not be ('completed)
      val r3 = Await.result(first3, 1100 milliseconds)
      r3.map(_.get) should equal (List(1, 3, 5))

      first4 should not be ('completed)
      val r4 = Await.result(first4, 500 milliseconds)
      r4 should equal ("hooray")

      futures.find(!_.isCompleted) should not be ('empty)
    }
  }
}
