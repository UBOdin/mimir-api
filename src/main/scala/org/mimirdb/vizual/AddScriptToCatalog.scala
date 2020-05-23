package org.mimirdb.vizual

import org.apache.spark.sql.DataFrame
import org.mimirdb.data.Catalog

object AddScriptToCatalog
{
  def apply(command: Seq[Command], catalog: Catalog, output: String): DataFrame = ???
}