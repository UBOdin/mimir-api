package org.mimirdb.api.tasks

import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI
import play.api.libs.json.JsValue

object CreateLens
{
  def supportedLenses = Seq[String]()

  def apply(
      sourceTable: String,
      outputTable: String,
      params: JsValue,
      lensType: String,
      humanReadableName: Option[String],
      sparkSession: SparkSession = MimirAPI.sparkSession
  ){
    ???
  }
}