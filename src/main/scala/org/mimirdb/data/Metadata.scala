package org.mimirdb.data

import org.apache.spark.sql.types._

object Metadata
{
  type MapSchema = Seq[(String, DataType)]
  type MapResource = (String, Seq[Any])

  def foldMapMigrations(migrations: Seq[MapMigration]): MapSchema = 
  {
    migrations.foldLeft(Seq[(String, DataType)]()) {
      case (_, InitMap(schema)) => schema
      case (prev, AddColumnToMap(column, t, _)) => prev :+ (column, t)
    }
  }
}

