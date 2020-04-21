package org.mimirdb.api

import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.apache.spark.serializer.KryoSerializer

object InitSpark
{
  def local: SparkSession =
  {
    SparkSession.builder
      .appName("Mimir-Caveat-Test")
      //.config("spark.ui.port", "4041")
      //.config("spark.eventLog.enabled", "true")
      //.config("spark.eventLog.longForm.enabled", "true")
      .config("spark.serializer", classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .master("local[*]")
      .getOrCreate()
  }
  
}