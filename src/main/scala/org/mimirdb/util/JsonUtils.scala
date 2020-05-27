package org.mimirdb.util

import play.api.libs.json.{JsPath, JsonValidationError}

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
}