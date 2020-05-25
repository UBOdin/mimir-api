package org.mimirdb.data

import play.api.libs.json._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{ SparkSession, DataFrame }


case class RangeConstructor(
  start: Long,
  end: Long,
  step: Long
)
  extends DataFrameConstructor
  with LazyLogging
{
  def construct(
    spark: SparkSession, 
    context: Map[String, DataFrame] = Map()
  ): DataFrame =
  {
    spark.range(start, end, step).toDF
  }
}

object RangeConstructor
  extends DataFrameConstructorCodec
{
  implicit val format: Format[RangeConstructor] = Json.format
  def apply(v: JsValue): DataFrameConstructor = v.as[RangeConstructor]
}