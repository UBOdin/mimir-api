package org.mimirdb.util

import scala.concurrent.{ Future, Await }
import scala.util.{Success, Failure}
import scala.concurrent.duration._
import java.util.concurrent.atomic.AtomicBoolean

import org.specs2.mutable.Specification


class TaskDeduplicatorSpec
  extends Specification
{
  lazy val deduplicate = new TaskDeduplicator[Int]()
  implicit val ec: scala.concurrent.ExecutionContext = 
    scala.concurrent.ExecutionContext.global

  "deduplicate tasks" >> {

    // Spin off a thread to process.  If this thread starts
    // first, we get a 1
    val xFuture = Future {
      deduplicate("TEST_1") { 
        Thread.sleep(2)
        1
      }    
    }

    // Compute TEST_1 again, but this time return a 2.
    val y = deduplicate("TEST_1") {
      Thread.sleep(2)
      2
    }

    // Recapture the spun-off result
    val x = Await.result(xFuture, 5.seconds)

    // Depending on which thread "wins" the race, we'll get either
    // a 1 or a 2.  Either is correct...
    x must beAnyOf(1, 2)

    // ... but we better get the same result for both threads.
    y must be equalTo x
  }

  "not deduplicate distinct aliases" >> {

    // as above, sipn off the x computation into a separate thread
    val x = Future {
      deduplicate("TEST_2") { Thread.sleep(1); 1 }
    }
    // compute the y value locally
    val y = deduplicate("TEST_3") { Thread.sleep(1); 2 }

    Await.result(x, 5.seconds) must be equalTo 1
    y must be equalTo 2
  }

  "not deduplicate sequential tasks" >> {

    // even though we're using the same alias, we wait for it to finish
    // before spinning off the next task here.  This isn't a critical 
    // behavior for the deduplicator, but it's how it works *right now* 
    // and it would be easy to introduce a bug if this behavior changes.
    val x = deduplicate("TEST_4") { 1 }
    val y = deduplicate("TEST_4") { 2 }

    x must be equalTo 1
    y must be equalTo 2
  }

  "not return until task side effects are applied" >> {
    val sideEffect = new AtomicBoolean(false)

    sideEffect.get must beFalse
    val x = deduplicate("TEST_5") {
        Thread.sleep(3)
        sideEffect.set(true)
        1
      }
    sideEffect.get must beTrue
    x must beEqualTo(1)
  }
}