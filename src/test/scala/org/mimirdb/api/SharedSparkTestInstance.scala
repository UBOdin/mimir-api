package org.mimirdb.api

import org.apache.spark.sql.{ SparkSession, DataFrame, Column }
import org.mimirdb.data.Catalog

object SharedSparkTestInstance
{
  lazy val spark = 
    SparkSession.builder
      .appName("Mimir-Caveat-Test")
      .master("local[*]")
      .getOrCreate()
  lazy val df = /* R(A int, B int, C int) */
    spark.range(0, 5)

  def initAPI {
    this.synchronized {
      if(MimirAPI.sparkSession == null){
        MimirAPI.sparkSession = spark
      }
      if(MimirAPI.catalog == null){
        MimirAPI.catalog = new Catalog("target/test.db", spark, "target/staged_files")
      }
    }
  }
}

trait SharedSparkTestInstance
{
  lazy val spark = SharedSparkTestInstance.spark
  lazy val df = SharedSparkTestInstance.df
}