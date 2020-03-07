package org.mimirdb.data

import org.apache.spark.sql.types._

sealed trait MapMigration

case class InitMap(schema: Metadata.MapSchema) extends MapMigration
case class AddColumnToMap(column: String, t: DataType, default: Option[Any]) extends MapMigration