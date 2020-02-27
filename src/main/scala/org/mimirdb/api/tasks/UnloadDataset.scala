package org.mimirdb.api.tasks

import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI
import java.io.File

object UnloadDataset
{
  def apply(
    sourceTable: String,
    url: String,
    format: String,
    sparkOptions: Seq[(String,String)],
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): Seq[File] = {
    ???
  }
}