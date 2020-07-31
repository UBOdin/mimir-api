package org.apache.spark.sql

import org.apache.spark.sql.execution.arrow.ArrowConverters
import org.apache.spark.sql.catalyst.InternalRow
import java.io.RandomAccessFile
import java.nio.channels.FileChannel
import org.apache.arrow.vector.ipc.ArrowStreamWriter
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.util.ArrowUtils
import org.apache.arrow.memory.RootAllocator

object ArrowProxy {
  
  def toArrow(df:DataFrame)  = {
    //ArrowConverters.toBatchIterator(df.toLocalIterator().asInstanceOf[Iterator[InternalRow]], df.schema, 100, "EST", org.apache.spark.TaskContext.get() )
    df.collectAsArrowToPython
  }
  
  val allocator = new RootAllocator(Integer.MAX_VALUE);
  
  def writeToMemoryFile(file:String, df:DataFrame) = {
    val schema = ArrowUtils.toArrowSchema(df.schema, "EST")
    val arrowOut = toArrow(df)
    val rafile = new RandomAccessFile(file, "rw")
    val out = rafile.getChannel()
                                        //.map(FileChannel.MapMode.READ_WRITE, 0, 2048);
    val bytesWritten = 0;
    val root = VectorSchemaRoot.create(schema, allocator)
  
    val writer = new ArrowStreamWriter(root, null, out)
    writer.start();
    writer.writeBatch();
    writer.end();
    val totalBytesWritten = writer.bytesWritten();
    println(totalBytesWritten)
  }
  
}