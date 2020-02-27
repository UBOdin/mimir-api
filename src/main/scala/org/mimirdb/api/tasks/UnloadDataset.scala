package org.mimirdb.api.tasks

import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI

object UnloadDataset
{
  def apply(
    /* table or view to unload */
      input: String,
    /* file url of datasorce to unload */
      file: String,
    /* format of file for spark */
      format: String,
    /* options for spark datasource api */
      backendOption: Seq[(String,String)],
      sparkSession: SparkSession = MimirAPI.sparkSession
  ): Seq[String] = ???

}