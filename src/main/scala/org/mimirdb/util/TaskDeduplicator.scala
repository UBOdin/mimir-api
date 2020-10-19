package org.mimirdb.util

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration.Duration
import scala.collection.concurrent.TrieMap 
import play.api.libs.json.JsValue
import java.util.concurrent.TimeUnit


/**
 * A deduplicated task processor.
 * 
 * Invoked as :
 * 
 *   val ret = 
 *     processor.deduplicate("My_Task") {
 *       // code goes here
 *     }
 * 
 * The provided block will be executed and the value returned by the
 * block will be returned from deduplicate.
 * 
 * Guaranteed:
 * - If another thread calls deduplicate("My_Task") { ... } before 
 *   the block finishes executing, the second thread will block until
 *   the first thread is done executing.  
 * - The return value of all blocked threads will be the value returned
 *   by the first thread.
 * 
 * Not Guaranteed
 * - If deduplicate(...) is called again after the passed block returns
 *   the block may be re-executed.
 */
class TaskDeduplicator[T]
{
  val pending = TrieMap[String, Future[T]]()
  val syncronizingContext = scala.concurrent.ExecutionContext
    .fromExecutor(java.util.concurrent.Executors.newSingleThreadExecutor())

  def apply(alias: String, duration: Duration = Duration.Inf)(op: => T): T =
    deduplicate(alias)(op)

  def deduplicate(alias: String, duration: Duration = Duration.Inf)(op: => T): T = 
  {
    val (needToDoCleanup, future) = 
      // atomically look up the provided alias in the pending
      // task index.  
      Await.result(Future[(Boolean, Future[T])] {
        (if(pending contains alias){
          // If present, it means the task is already
          // being processed.
          false
        } else {
          // If not present, we have to create the task
          pending += alias -> Future[T] {
            val ret = op
            op
          }(
            scala.concurrent.ExecutionContext.global
          )
          true
        }, pending(alias))
      }( syncronizingContext ), 
      Duration.apply(10, TimeUnit.SECONDS))

    val result = Await.result(future, duration)
    if(needToDoCleanup){
      pending -= alias
    }
    return result
  }
}