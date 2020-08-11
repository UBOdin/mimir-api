package org.mimirdb.api

import java.io.OutputStream
import play.api.libs.json._
import org.apache.spark.sql.types.StructField
import org.mimirdb.spark.Schema.fieldFormat
import javax.servlet.http.HttpServletResponse
import com.typesafe.scalalogging.LazyLogging

abstract class Response 
{
  def write(output: HttpServletResponse)
}

abstract class BytesResponse
  extends Response
{
  def contentType = "text/plain"
  def getBytes: Array[Byte]
  def write(output: HttpServletResponse)
  {
    output.setHeader("Content-Type", contentType)
    val os = output.getOutputStream()
    os.write(getBytes)
    os.flush()
    os.close() 
  }
}

abstract class JsonResponse[R](implicit format: Format[R])
  extends BytesResponse
  with LazyLogging
{
  override def contentType = "application/json"
  def getBytes = 
  {
    val response = Json.stringify(Json.toJson(this.asInstanceOf[R]))
    logger.trace(s"RESPONSE: $response")
    response.getBytes()
  }
}


case class ErrorResponse (
            /* throwable class name */
                  errorType: String,
            /* throwable message */
                  errorMessage: String,
            /* throwable stack trace */
                  stackTrace: String,
            /* error code */
                  status: Int = HttpServletResponse.SC_BAD_REQUEST
) extends JsonResponse[ErrorResponse]
{
  override def write(output: HttpServletResponse)
  { 
    output.setStatus(status)
    super.write(output)
  }
}

object ErrorResponse {
  implicit val format: Format[ErrorResponse] = Json.format

  def apply(error: Throwable, message: String): ErrorResponse = 
    apply(error, message, null)
  def apply(error: Throwable, message: String, className: String): ErrorResponse = 
    ErrorResponse(
      Option(className).getOrElse { error.getClass.getCanonicalName() } ,
      message,
      error.getStackTrace.map(_.toString).mkString("\n")
    )
}


case class LensList (
    lensTypes: Seq[String]
) extends JsonResponse[LensList]

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
) extends JsonResponse[CreateResponse]

object CreateResponse {
  implicit val format: Format[CreateResponse] = Json.format
}

