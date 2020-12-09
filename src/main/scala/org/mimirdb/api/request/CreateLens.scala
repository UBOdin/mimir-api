package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructField
import com.typesafe.scalalogging.LazyLogging

import org.mimirdb.api.{ Request, JsonResponse, MimirAPI }
import org.mimirdb.data.{ DataFrameConstructor, DataFrameConstructorCodec }
import org.mimirdb.lenses.{ Lenses, LensConstructor }
import org.mimirdb.spark.Schema
import org.mimirdb.spark.Schema.fieldFormat

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
                  resultName: Option[String],
            /* optional properties */
                  properties: Option[Map[String,JsValue]]
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
        humanReadableName.getOrElse { input }
      ),
      Set(input),
      properties = properties.getOrElse { Map.empty }
    )
    CreateLensResponse(output, config)
  }
}

object CreateLensRequest 
{
  implicit val format: Format[CreateLensRequest] = Json.format
}

case class CreateLensResponse (
            /* name of resulting lens */
                  name: String,
                  config: JsValue,
                  schema: Seq[StructField],
                  properties: Map[String, JsValue]
) extends JsonResponse[CreateLensResponse]

object CreateLensResponse {
  implicit val format: Format[CreateLensResponse] = Json.format

  def apply(output: String, config: JsValue): CreateLensResponse =
    CreateLensResponse(
      output, 
      config, 
      MimirAPI.catalog.get(output).schema.fields,
      Map.empty
    )
}
