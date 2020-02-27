package org.mimirdb.api

import play.api.libs.json._

object SparkPrimitive
{
  def encode(k: Any): JsValue = ???
  def decode(k: JsValue): Any = ???
}
