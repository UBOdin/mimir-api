package org.mimirdb.api

import org.apache.spark.sql.{ SparkSession, DataFrame, Column }
import org.mimirdb.data.{ Catalog, LoadConstructor }
import org.mimirdb.lenses.Lenses
import org.mimirdb.lenses.implementation.TestCaseGeocoder

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

        // And load up some example test data
        MimirAPI.catalog.put(
          "TEST_R",
          LoadConstructor(
            url = "test_data/r.csv",
            format = "csv",
            sparkOptions = Map("header" -> "true")
          ),
          Set()
        )

        MimirAPI.catalog.put(
          "GEO",
          LoadConstructor(
            url = "test_data/geo.csv",
            format = "csv",
            sparkOptions = Map("header" -> "true")
          ),
          Set()
        )

        Lenses.initGeocoding(
          Seq(TestCaseGeocoder),
          MimirAPI.catalog
        )

      }
    }
  }
}

trait SharedSparkTestInstance
{
  lazy val spark = SharedSparkTestInstance.spark
  lazy val df = SharedSparkTestInstance.df
}