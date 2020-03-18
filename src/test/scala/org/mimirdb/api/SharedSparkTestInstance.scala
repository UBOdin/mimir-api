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

  def loadCSV(name: String, url: String, header: Boolean = true)
  {
    MimirAPI.catalog.put(name, LoadConstructor(url = url, format = "csv", 
      sparkOptions = Map("header" -> header.toString)), Set())
  }

  def initAPI {
    this.synchronized {
      if(MimirAPI.sparkSession == null){
        MimirAPI.sparkSession = spark
      }
      if(MimirAPI.catalog == null){
        MimirAPI.catalog = new Catalog("target/test.db", spark, "target/staged_files")

        // And load up some example test data
        loadCSV("TEST_R", "test_data/r.csv")
        loadCSV("GEO", "test_data/geo.csv")
        loadCSV("SEQ", "test_data/seq.csv")

        // Finally, initialize geocoding with the test harness
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

  def dataset(name:String) = MimirAPI.catalog.get(name)
}