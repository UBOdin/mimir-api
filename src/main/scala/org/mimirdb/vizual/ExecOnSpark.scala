package org.mimirdb.vizual

import org.apache.spark.sql.DataFrame
import org.mimirdb.data.Catalog

object ExecOnSpark
{
  def apply(command: Seq[Command], views: Map[String, DataFrame]): DataFrame = ???
}