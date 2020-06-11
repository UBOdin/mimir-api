package org.mimirdb.data

import org.specs2.mutable.Specification
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.LongType

class MetadataBackendSpec 
  extends Specification
{

  lazy val metadata = new JDBCMetadataBackend("sqlite", "target/catalog.db")
  
  "The MetadataBackend" >> {
    "Store Simple metadata" >>
    {
      metadata.registerMap("test", Seq(InitMap(Seq(
            ("id",StringType), ("permissions",LongType))))).put("test", Seq("theid",1L))
      ok
    }
  }

}