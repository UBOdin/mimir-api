package org.mimirdb.profiler

import play.api.libs.json.{ Json, JsValue }
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.util.TimerUtils
import org.apache.spark.sql.functions.{ lit, sum }
class DataProfilerSpec
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  def beforeAll(): Unit = SharedSparkTestInstance.initAPI

  "Basic Profiling" >> {

    val datasetProperties = 
      TimerUtils.logTime("Data Profiler on TEST_R") {
        DataProfiler(MimirAPI.catalog.get("TEST_R"))
      }

    println(Json.toJson(datasetProperties))


    datasetProperties must haveKey("count")
    datasetProperties("count").as[Int] must be equalTo(7)
    
    datasetProperties must haveKey("columns")    
    val columnProperties = datasetProperties("columns").as[Seq[Map[String,JsValue]]]

    columnProperties must haveSize(3) 
    columnProperties.map { _ must haveKey("column") }
    columnProperties.map { 
      _("column").as[Map[String,JsValue]].get("name").get.as[String] 
    } must contain(eachOf("A", "B", "C"))
    columnProperties.map { 
      _("column").as[Map[String,JsValue]].get("type").get.as[String] 
    } must contain("string")
  }

  "Larger Tables" >> {

    val df = spark.read.csv("test_data/cureSource.csv")
    println(s"Large Table Rows: ${df.agg(sum(lit(1l))).take(1).head.getLong(0)}")

    val (properties, timeNanos) = 
      TimerUtils.time {
        DataProfiler(df)
      }
    println(s"Data Profiler on cureSource took ${timeNanos / 1000000.0} ms")

    properties("count").as[Long] must be equalTo(df.count())

  }
}