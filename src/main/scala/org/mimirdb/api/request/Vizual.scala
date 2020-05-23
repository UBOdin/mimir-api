package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.SparkSession

import org.mimirdb.api.{ Request, Response, MimirAPI }
import org.mimirdb.vizual.{ Command, AddScriptToCatalog }
import org.mimirdb.data.VizualScriptConstructor

case class VizualRequest (
  input: String,
  script: Seq[Command],
  resultName: Option[String]
) 
  extends Request 
{

  lazy val output = 
    resultName.getOrElse {
      val viewNameBase = (input.toString + script.mkString("\n")).hashCode()
      "VIZUAL_" + (viewNameBase.toString().replace("-", ""))
    }

  def handle = {
    AddScriptToCatalog(script, MimirAPI.catalog, output)
    Json.toJson(VizualResponse(output, script))
  }
}

object VizualRequest 
{
  implicit val format: Format[VizualRequest] = Json.format
}

case class VizualResponse (
            /* name of resulting lens */
                  lensName: String,
            /* revised/simplified script */
                  script: Seq[Command]
) extends Response

object VizualResponse {
  implicit val format: Format[VizualResponse] = Json.format
}
