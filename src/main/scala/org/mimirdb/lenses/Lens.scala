package org.mimirdb.lenses

import play.api.libs.json._
import org.apache.spark.sql.DataFrame

trait Lens
{
  def train(input: DataFrame, rawConfig:JsValue): JsValue
  def create(input: DataFrame, config: JsValue, context: String): DataFrame
  def apply(input: DataFrame, config: JsValue, context: String) = 
    create(input, config, context)
}