package org.mimirdb.api.tasks

import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI

object CreateView
{
  def apply(
    query: String,
    targetTable: String,
    temporaryTables: Map[String,String] = Map(),
    sparkSession: SparkSession = MimirAPI.sparkSession
  ){ 
    ???
  }
}