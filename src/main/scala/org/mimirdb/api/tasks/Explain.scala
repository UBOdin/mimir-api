package org.mimirdb.api.tasks

import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI
import org.mimirdb.caveats.{ Caveat, CaveatSet }

object Explain
{
  def apply(
    query: String, 
    rows: Seq[String] = null,
    cols: Seq[String] = null,
    schemaCaveats: Boolean = true,
    reasonCap: Int = 3,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): Seq[Caveat] = ???

  def coarsely(
    query: String, 
    rows: Seq[String] = null,
    cols: Seq[String] = null,
    schemaCaveats: Boolean = true,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): Seq[CaveatSet] = ???
}