package org.mimirdb.api

import play.api.libs.json._
import org.mimirdb.caveats.{ CaveatSet, Caveat }
import org.mimirdb.api.CaveatFormat._

sealed abstract class Response {
  
}

object Response {
  
}

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
}

case class CodeEvalResponse (
            /* stdout from evaluation of scala code */
                  stdout: String,
            /* stderr from evaluation of scala code */
                  stderr: String
) extends Response

object CodeEvalResponse {
  implicit val format: Format[CodeEvalResponse] = Json.format
}


case class LoadResponse (
            /* name of resulting table */
                  name: String
) extends Response

object LoadResponse {
  implicit val format: Format[LoadResponse] = Json.format
}


case class UnloadResponse (
            /* name of resulting table */
                  outputFiles: Seq[String]
) extends Response

object UnloadResponse {
  implicit val format: Format[UnloadResponse] = Json.format
}

case class CreateLensResponse (
            /* name of resulting lens */
                  lensName: String
) extends Response

object CreateLensResponse {
  implicit val format: Format[CreateLensResponse] = Json.format
}


case class CreateViewResponse (
            /* name of resulting view */
                  viewName: String
) extends Response

object CreateViewResponse {
  implicit val format: Format[CreateViewResponse] = Json.format
}


case class CreateAdaptiveSchemaResponse (
            /* name of resulting adaptive schema */
                  adaptiveSchemaName: String
) extends Response

object CreateAdaptiveSchemaResponse {
  implicit val format: Format[CreateAdaptiveSchemaResponse] = Json.format
}


case class ExplainResponse (
                  reasons: Seq[Caveat]
) extends Response

object ExplainResponse {
  implicit val format: Format[ExplainResponse] = Json.format
}


case class DataContainer (
                  schema: Seq[Schema],
                  data: Seq[Seq[Any]],
                  prov: Seq[String],
                  colTaint: Seq[Seq[Boolean]],
                  rowTaint: Seq[Boolean],
                  reasons: Seq[Seq[Caveat]]
) extends Response

object DataContainer {
  implicit val format: Format[DataContainer] = Format(
    new Reads[DataContainer] {
      def reads(data: JsValue): JsResult[DataContainer] =
      {
        val parsed = data.as[Map[String,JsValue]]
        JsSuccess(
          DataContainer(
            parsed("schema").as[Seq[Schema]],
            parsed("data").as[Seq[Seq[JsValue]]].map { _.map { SparkPrimitive.decode(_) } },
            parsed("prov").as[Seq[String]],
            parsed("colTaint").as[Seq[Seq[Boolean]]],
            parsed("rowTaint").as[Seq[Boolean]],
            parsed("reasons").as[Seq[Seq[Caveat]]]
          )
        )
      }
    },
    new Writes[DataContainer] { 
      def writes(data: DataContainer): JsValue =
        Json.obj(
          "schema" -> data.schema,
          "data" -> data.data.map { _.map { SparkPrimitive.encode(_) } },
          "prov" -> data.prov,
          "colTaint" -> data.colTaint,
          "rowTaint" -> data.rowTaint,
          "reasons" -> data.reasons
        )
    }
  )
}


case class LensList (
    lensTypes: Seq[String]
) extends Response

object LensList {
  implicit val format: Format[LensList] = Json.format
}


case class AdaptiveSchemaList (
    adaptiveSchemaTypes: Seq[String]
) extends Response

object AdaptiveSchemaList {
  implicit val format: Format[AdaptiveSchemaList] = Json.format
}


case class SchemaList (
    schema: Seq[Schema]
) extends Response

object SchemaList {
  implicit val format: Format[SchemaList] = Json.format
}




case class CreateSampleResponse (
            /* name of resulting view */
                  viewName: String
) extends Response

object CreateSampleResponse {
  implicit val format: Format[CreateSampleResponse] = Json.format
}
