package org.mimirdb.api.tasks

import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI

object CreateView
{
  def apply(
    /* temporary view definitions for use in creating the view */
          input: Map[String,String],
    /* query for view */
          query: String,
    /* optionally provide an output name */
      resultName: Option[String],
      sparkSession: SparkSession = MimirAPI.sparkSession
  ): String = ???
}