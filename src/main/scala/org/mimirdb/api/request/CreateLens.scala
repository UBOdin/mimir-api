package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.LazyLogging

import org.mimirdb.api.{ Request, Response, MimirAPI, Schema }
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
  with LazyLogging
{

  lazy val output = 
    resultName.getOrElse {
      val lensNameBase = (input.toString + `type` + params.toString).hashCode()
      "LENS_" + `type` + "_" + (lensNameBase.toString().replace("-", ""))
    }

  def handle = {
    val df = MimirAPI.catalog.get(input)
    logger.trace(s"Training $input ---[${`type`}]---> $output")
    val config = Lenses(`type`).train(df, params)
    logger.trace(s"Saving lens")
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
    Json.toJson(CreateLensResponse(output, config))
  }
}

object CreateLensRequest 
{
  implicit val format: Format[CreateLensRequest] = Json.format
}

case class CreateLensResponse (
            /* name of resulting lens */
                  lensName: String,
                  config: JsValue,
                  schema: Seq[Schema]
) extends Response

object CreateLensResponse {
  implicit val format: Format[CreateLensResponse] = Json.format

  def apply(output: String, config: JsValue): CreateLensResponse =
    CreateLensResponse(
      output, 
      config, 
      Schema(MimirAPI.catalog.get(output).schema)
    )
}
