package org.mimirdb.api.tasks

import org.mimirdb.api.CodeEvalResponse
import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI

object EvalScala
{
  def apply(
    source: String,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): CodeEvalResponse = ???
}