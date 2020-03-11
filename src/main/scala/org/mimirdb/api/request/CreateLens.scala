package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.SparkSession

import org.mimirdb.api.{ Request, Response, MimirAPI }
import org.mimirdb.data.{ DataFrameConstructor, DataFrameConstructorCodec }
import org.mimirdb.lenses.{ Lenses, LensConstructor }

case class CreateLensRequest (
            /* input for lens */
                  input: String,
            /* configuration parameters */
                  params: JsValue,
            /* type name of lens */
                  `type`: String,
            /* materialize input before creating lens */
                  materialize: Boolean,
            /* optional human-readable name */
                  humanReadableName: Option[String],
            /* optional name for the result table */
                  resultName: Option[String]
) 
  extends Request 
{

  lazy val output = 
    resultName.getOrElse {
      val lensNameBase = (input.toString + `type` + params.toString).hashCode()
      "LENS_" + `type` + "_" + (lensNameBase.toString().replace("-", ""))
    }

  def handle = {
    val df = MimirAPI.catalog.get(input)
    val config = Lenses(`type`).train(df, params)
    MimirAPI.catalog.put(
      output,
      LensConstructor(
        `type`, 
        input, 
        config, 
        "in " +humanReadableName.getOrElse { input }
      ),
      Set(input)
    )
    Json.toJson(CreateLensResponse(output))
  }
}

object CreateLensRequest 
{
  implicit val format: Format[CreateLensRequest] = Json.format
}

case class CreateLensResponse (
            /* name of resulting lens */
                  lensName: String
) extends Response

object CreateLensResponse {
  implicit val format: Format[CreateLensResponse] = Json.format
}
