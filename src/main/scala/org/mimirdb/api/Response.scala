package org.mimirdb.api

import play.api.libs.json._
import org.apache.spark.sql.types.StructField
import org.mimirdb.spark.Schema.fieldFormat

abstract class Response 

case class ErrorResponse (
            /* throwable class name */
                  errorType: String,
            /* throwable message */
                  errorMessage: String,
            /* throwable stack trace */
                  stackTrace: String
) extends Response

object ErrorResponse {
  implicit val format: Format[ErrorResponse] = Json.format

  def apply(error: Throwable, message: String, className: String = null): ErrorResponse = 
    ErrorResponse(
      Option(className).getOrElse { error.getClass.getCanonicalName() } ,
      message,
      error.getStackTrace.map(_.toString).mkString("\n")
    )
}

case class LensList (
    lensTypes: Seq[String]
) extends Response

object LensList {
  implicit val format: Format[LensList] = Json.format
}

case class CreateResponse (
            /* name of resulting table */
                  name: String,
            /* schema of resulting table */
                  schema: Seq[StructField],
            /* properties assocated with the resulting table */
                  properties: Map[String, JsValue]
) extends Response

object CreateResponse {
  implicit val format: Format[CreateResponse] = Json.format
}

