package org.mimirdb.data

import play.api.libs.json.{ JsValue, Format }
import org.apache.spark.sql.{ SparkSession, DataFrame } 

trait Constructor 
{
  def construct(spark: SparkSession, context: Map[String,DataFrame]): DataFrame

  // Assume a default companion object with a format
  def deserializer = getClass.getName + "$"
}

trait ConstructorCodec
{
  def apply(j: JsValue): Constructor
}