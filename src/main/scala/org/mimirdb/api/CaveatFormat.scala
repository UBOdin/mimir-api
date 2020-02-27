package org.mimirdb.api

import play.api.libs.json._
import org.mimirdb.caveats.{ Caveat, CaveatSet }

object CaveatFormat
{
  implicit val caveatFormat: Format[Caveat] = Format(
    new Reads[Caveat] {
      def reads(v: JsValue): JsResult[Caveat] = {
        val fields = v.as[Map[String, JsValue]]
        JsSuccess(Caveat(
          fields("message").as[String],
          fields.get("family").map { _.as[String] },
          fields("key").as[Seq[JsValue]].map { SparkPrimitive.decode(_) }
        ))
      }
    },
    new Writes[Caveat] { 
      def writes(caveat: Caveat): JsValue = {
        Json.obj(
          "message" -> JsString(caveat.message),
          "family" -> caveat.family.map { JsString(_) }.getOrElse[JsValue] { JsNull },
          "key" -> JsArray(caveat.key.map { SparkPrimitive.encode(_) })
        )
      }
    }

  )
  implicit val caveatSetFormat: Format[CaveatSet] = Format(???, ???)
}