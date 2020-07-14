package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField

import org.mimirdb.api.{ Request, Response, MimirAPI }
import org.mimirdb.vizual.{ Command, VizualScriptConstructor }
import org.mimirdb.spark.Schema
import org.mimirdb.spark.Schema.fieldFormat

case class VizualRequest (
  input: String,
  script: Seq[Command],
  resultName: Option[String],
  compile: Option[Boolean]
) 
  extends Request 
{

  lazy val output = 
    resultName.getOrElse {
      val viewNameBase = (input.toString + script.mkString("\n")).hashCode()
      "VIZUAL_" + (viewNameBase.toString().replace("-", ""))
    }

  def handle = {
    // ignore simplification code for the moment
    val simplified = 
      if(compile.getOrElse(false)){
        throw new RuntimeException("Vizual compilation unsupported for now")
      } else {
        script
      }

    // Just add to the catalog and move on (we can get fancier later)
    val df = MimirAPI.catalog.put(
      output,
      VizualScriptConstructor(simplified, input),
      Set(input),
      true
    )
    Json.toJson(VizualResponse(output, simplified, Schema(df), Map.empty))
  }
}

object VizualRequest 
{
  implicit val format: Format[VizualRequest] = Json.format
}

case class VizualResponse (
            /* name of resulting view */
                  name: String,
            /* revised/simplified script */
                  script: Seq[Command],
            /* resulting schema */
                  schema: Seq[StructField],
            /* any properties associated with the result */
                  properties: Map[String, JsValue]
) extends Response

object VizualResponse {
  implicit val format: Format[VizualResponse] = Json.format
}
