package org.mimirdb.lenses

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.mimirdb.data.{ DataFrameConstructor, DataFrameConstructorCodec, DefaultProvenance }

case class LensConstructor(
  name: String,
  input: String,
  config: JsValue,
  context: String
) extends DataFrameConstructor
  with DefaultProvenance
{
  def construct(spark: SparkSession, evalContext: Map[String, () => DataFrame]): DataFrame =
    Lenses(name.toLowerCase()).create(evalContext(input)(), config, context)
}

object LensConstructor 
  extends DataFrameConstructorCodec
{
  implicit val format: Format[LensConstructor] = Json.format
  def apply(j: JsValue) = j.as[LensConstructor]
}