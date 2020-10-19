package org.mimirdb.blobs

import com.typesafe.scalalogging.LazyLogging
import org.mimirdb.data.{ MetadataBackend, InitMap }
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types._
import org.mimirdb.spark.PythonUDFBuilder
import scala.collection.concurrent.TrieMap

class BlobStore(
  metadata: MetadataBackend, 
  python: PythonUDFBuilder
) extends LazyLogging
{
  val blobs = metadata.registerMap("MIMIR_BLOBS", Seq(
    InitMap(Seq(
      "B_TYPE"         -> StringType,
      "B_DATA"         -> BinaryType
    ))
  ))

  val udfCache = TrieMap[String, (Seq[Expression] => Expression)]()

  def put(name: String, blobType: String, data: Array[Byte])
  {
    blobs.put(name, Seq(blobType, data))
  }

  def get(name: String): Option[(String, Array[Byte])] =
  {
    blobs.get(name).map { record => 
      (
        record._2(0).asInstanceOf[String],
        record._2(1).asInstanceOf[Array[Byte]]
      )
    }
  }

  def getPythonUDF(name: String): Option[Seq[Expression] => Expression] =
  {
    if(!blobs.exists(name)) { return None }
    else { 
      return Some(
        udfCache.getOrElseUpdate(
          name,
          { 
            val (_, code) = get(name).get
            python(new String(code))
          }
        )
      )
    }
  }
}