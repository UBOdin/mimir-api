package org.mimirdb.api

import org.apache.spark.sql.SparkSession

object InitSpark
{
  def local: SparkSession =
  {
    SparkSession.builder
      .appName("Mimir-Caveat-Test")
      .master("local[*]")
      .getOrCreate()
  }
}