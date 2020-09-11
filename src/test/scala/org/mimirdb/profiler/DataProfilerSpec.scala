package org.mimirdb.profiler

import play.api.libs.json.{ Json, JsValue }
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.util.TimerUtils

class DataProfilerSpec
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  def beforeAll(): Unit = SharedSparkTestInstance.initAPI

  "The Data Profiler" >> {

    val datasetProperties = 
      TimerUtils.logTime("Run Data Profiler") {
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
}