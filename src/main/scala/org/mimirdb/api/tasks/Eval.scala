package org.mimirdb.api.tasks

import org.mimirdb.api.CodeEvalResponse
import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI

object Eval
{
  def R(
    source: String, 
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): CodeEvalResponse = ???

  def Scala(
    source: String,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): CodeEvalResponse = ???
}