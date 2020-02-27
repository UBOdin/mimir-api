package org.mimirdb.api.tasks

import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI

object LoadDataset
{
  def apply(
    url: String,
    targetTable: String,
    format: String,
    inferTypes: Boolean,
    detectHeaders: Boolean,
    humanReadableName: Option[String],
    sparkOptions: Seq[(String,String)],
    dependencies: Seq[String],
    sparkSession: SparkSession = MimirAPI.sparkSession
  ){
    ???
  }
}