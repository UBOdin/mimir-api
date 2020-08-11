package org.mimirdb.util

import java.io.{ InputStream, IOException }
import scala.collection.mutable
import java.util.Arrays

object StreamUtils
{
  def readAll(stream: InputStream): Array[Byte] =
  {
    //val data = mutable.Buffer[(Int, Array[Byte])]()
    val data = mutable.Buffer[Int]()
    var bytesRead = 0
    while(bytesRead >= 0){
      try {
        /*val target = stream.available()
        if(target > 0) {
          val buffer:Array[Byte] = new Array[Byte](target)
          bytesRead = stream.read(buffer)
          if(bytesRead > 0){
            data.append( bytesRead -> buffer )
          }
        } else {
          bytesRead = target
          Thread.sleep(100)
        }*/
        bytesRead = stream.read()
        if(bytesRead > 0){
          data.append( bytesRead )
        }
      } catch {
        case _:IOException => bytesRead = -1
      }
    }
    // Shortcut 1: If we didn't read anything, return an empty array
    if(data.size == 0) { return Array[Byte]() }
    else return data.map(i => i.toByte).toArray
    /*if(data.size == 1) { 
      val (count, block) = data(0)
      // Shortcut 2: If the block is exactly correct, return it
      if(block.size <= count) { return block }
      // Shortcut 3: If we're returninng a subset of the array, do so
      return Arrays.copyOfRange(block, 0, count)
    }

    // Slow path: Allocate a new array with all the data and copy into it
    val ret = new Array[Byte](data.map { _._1 }.sum)
    var idx = 0
    for( (count, block) <- data ){
      System.arraycopy(block, 0, ret, idx, count)
      idx += count
    }
    return ret*/
  }
}