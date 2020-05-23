package org.mimirdb.data

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.mimirdb.vizual.{ Vizual, Command }

case class VizualScriptConstructor(
  input: String,
  script: Seq[Command]
)
  extends DataFrameConstructor
{
  def construct(spark: SparkSession, context: Map[String,DataFrame]): DataFrame =
  {
    throw new RuntimeException("Vizual Scripts are temporarily disabled")
    // Vizual(script, context(input))
  }
}

object VizualScriptConstructor 
  extends DataFrameConstructorCodec
{
  implicit val format: Format[VizualScriptConstructor] = Json.format
  def apply(j: JsValue) = j.as[VizualScriptConstructor]
}