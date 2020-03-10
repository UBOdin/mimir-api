package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.SparkSession

import org.mimirdb.api.{ Request, Response }
import org.mimirdb.api.MimirAPI


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
) extends Request {

  lazy val output = 
    resultName.getOrElse {
      val lensNameBase = (input.toString + `type` + params.toString).hashCode()
      "LENS_" + `type` + "_" + (lensNameBase.toString().replace("-", ""))
    }

  def handle = {
    construct(
      input,
      output,
      params,
      `type`,
      humanReadableName
    )
    Json.toJson(CreateLensResponse(output))
  }

  def construct(
      sourceTable: String,
      outputTable: String,
      params: JsValue,
      lensType: String,
      humanReadableName: Option[String],
      sparkSession: SparkSession = MimirAPI.sparkSession
  ){
    ???
  }
}

object CreateLensRequest {
  val supportedLenses = Seq[String]()
  implicit val format: Format[CreateLensRequest] = Json.format
}

case class CreateLensResponse (
            /* name of resulting lens */
                  lensName: String
) extends Response

object CreateLensResponse {
  implicit val format: Format[CreateLensResponse] = Json.format
}
