package org.mimirdb.api

import play.api.libs.json._
import org.mimirdb.caveats.{ Caveat }
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.DataType
import org.mimirdb.spark.SparkPrimitive


object CaveatFormat
{
  private implicit val literalFormat: Format[Literal] = Format(
    new Reads[Literal] { 
      def reads(literal: JsValue): JsResult[Literal] = {
        val fields = literal.as[Map[String,JsValue]]
        // Note, we *want* the quotation marks and escapes on the following 
        // line, since spark annoyingly hides the non-json version from us.
        val t = DataType.fromJson(fields("dataType").toString) 
        JsSuccess(
          Literal(SparkPrimitive.decode(fields("value"), t), t)
        )
      }
    },
    new Writes[Literal] {
      def writes(literal: Literal): JsValue = {
        Json.obj( 
          "dataType" -> literal.dataType.typeName,
          "value" -> SparkPrimitive.encode(literal.value, literal.dataType)
        )
      }
    }
  )

  implicit val caveatFormat: Format[Caveat] = Format(
    new Reads[Caveat] {
      def reads(j: JsValue): JsResult[Caveat] = {
        val fields = j.as[Map[String, JsValue]]
        JsSuccess(Caveat(
          message = fields.getOrElse("english", { return JsError() }).as[String],
          family = fields.get("family").map { _.as[String] },
          key = fields.get("args").map { _.as[Seq[Literal]] }.getOrElse { Seq() }
        ))
      }
    },
    new Writes[Caveat] {
      def writes(c: Caveat): JsValue = {
        Json.obj(
          "english" -> JsString(c.message),
          "args" -> Json.toJson(c.key),
          "source" -> Json.toJson(c.family)
        )
      }
    }
  )
}