package org.mimirdb.util

import scala.concurrent.{ Future, Await }
import scala.concurrent.duration.Duration
import scala.collection.concurrent.TrieMap 
import play.api.libs.json.JsValue


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

  def apply(alias: String, duration: Duration = Duration.Inf)(op: => T): T =
    deduplicate(alias)(op)

  def deduplicate(alias: String, duration: Duration = Duration.Inf)(op: => T): T = 
  {
    val (future, needToDoCleanup) = 
      // atomically look up the provided alias in the pending
      // task index.  
      synchronized {
        if(pending contains alias){
          // If present, it means the task is already
          // being processed.
          (pending(alias), false)
        } else {
          // If not present, we have to create the task
          val future = Future[T] {
            val ret = op
            op
          }(
            scala.concurrent.ExecutionContext.global
          )
          pending += alias -> future
          (future, true)
        }
      }

    val result = Await.result(future, duration)
    if(needToDoCleanup){
      pending -= alias
    }
    return result
  }
}