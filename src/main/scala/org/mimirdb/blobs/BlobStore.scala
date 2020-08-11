package org.mimirdb.blobs

import com.typesafe.scalalogging.LazyLogging
import org.mimirdb.data.{ MetadataBackend, InitMap }
import org.apache.spark.sql.types._

class BlobStore(
  metadata: MetadataBackend, 
) extends LazyLogging
{
  val blobs = metadata.registerMap("MIMIR_BLOBS", Seq(
    InitMap(Seq(
      "B_TYPE"         -> StringType,
      "B_DATA"         -> BinaryType
    ))
  ))

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
}