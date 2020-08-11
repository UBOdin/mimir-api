package org.mimirdb.api

import org.apache.spark.sql.SparkSession
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geosparkviz.core.Serde.GeoSparkVizKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator
import org.mimirdb.caveats.Caveats

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
      .config("spark.kryo.registrator", classOf[GeoSparkVizKryoRegistrator].getName)
      .master("local[*]")
      .getOrCreate()
  }

  def initPlugins(sparkSession: SparkSession)
  {
    GeoSparkSQLRegistrator.registerAll(sparkSession)
    GeoSparkVizRegistrator.registerAll(sparkSession)
    System.setProperty("geospark.global.charset", "utf8")
    Caveats.registerAllUDFs(sparkSession)
  }

}
