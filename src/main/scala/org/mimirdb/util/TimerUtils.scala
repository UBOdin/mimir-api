package org.mimirdb.util

trait TimerUtils 
{
  protected def logger: com.typesafe.scalalogging.Logger

  def time[F](anonFunc: => F): (F, Long) = 
  {  
    val tStart = System.nanoTime()
    val anonFuncRet:F = anonFunc  
    val tEnd = System.nanoTime()
    (anonFuncRet, tEnd-tStart)
  }  

  def logTime[F](
    descriptor: String, 
    context: String = "",
    log:(String => Unit) = logger.debug(_)
  )(anonFunc: => F): F = 
  {
    val (anonFuncRet, nanoTime) = time(anonFunc)
    val sep = if(context.equals("")){""} else {" "}
    log(s"$descriptor: $nanoTime ns$sep$context")
    anonFuncRet
  }
}