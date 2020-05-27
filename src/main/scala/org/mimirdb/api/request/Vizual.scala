package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.SparkSession

import org.mimirdb.api.{ Request, Response, MimirAPI }
import org.mimirdb.vizual.{ Command, VizualScriptConstructor }

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
    MimirAPI.catalog.put(
      output,
      VizualScriptConstructor(simplified, input),
      Set(input),
      true
    )
    Json.toJson(VizualResponse(output, simplified))
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
                  script: Seq[Command]
) extends Response

object VizualResponse {
  implicit val format: Format[VizualResponse] = Json.format
}
