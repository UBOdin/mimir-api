package org.mimirdb.vizual

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.mimirdb.data.{ DataFrameConstructor, DataFrameConstructorCodec }

case class VizualScriptConstructor(
  script: Seq[Command],
  input: String
)
  extends DataFrameConstructor
{
  def construct(spark: SparkSession, context: Map[String,DataFrame]): DataFrame =
    ExecOnSpark(context(input), script)
}

object VizualScriptConstructor 
  extends DataFrameConstructorCodec
{
  implicit val format: Format[VizualScriptConstructor] = Json.format
  def apply(j: JsValue) = j.as[VizualScriptConstructor]
}