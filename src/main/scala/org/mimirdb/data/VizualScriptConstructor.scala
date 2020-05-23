package org.mimirdb.data

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.mimirdb.vizual.{ ExecOnSpark, Command }

case class VizualScriptConstructor(
  script: Seq[Command]
)
  extends DataFrameConstructor
{
  def construct(spark: SparkSession, context: Map[String,DataFrame]): DataFrame =
    ExecOnSpark(script, context)
}

object VizualScriptConstructor 
  extends DataFrameConstructorCodec
{
  implicit val format: Format[VizualScriptConstructor] = Json.format
  def apply(j: JsValue) = j.as[VizualScriptConstructor]
}