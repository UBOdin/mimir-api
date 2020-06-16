package org.mimirdb.util

import play.api.libs.json.{JsValue, Json, JsPath, JsonValidationError}

object JsonUtils
{
  def stringifyJsonParseErrors(errors: Seq[(JsPath, Seq[JsonValidationError])]): Seq[String] =
  {
    errors.flatMap { case (path, errorsAtPath) =>
      errorsAtPath.map {
        case v: JsonValidationError => 
          v.messages.mkString("; ") + 
            (if(v.args == null){ "" } else { " (" + v.args.toString + ") "}) +
            "(at $" + path.toString + ")"
      }
    }
  }

  def addFieldsToObject(j: JsValue, fields: (String, JsValue)*): JsValue =
    Json.toJson(j.as[Map[String, JsValue]] ++ fields.toMap)
}