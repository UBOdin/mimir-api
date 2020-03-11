package org.mimirdb.lenses.implementation

import play.api.libs.json._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ DataType, StringType }
import org.mimirdb.lenses.Lens
import org.mimirdb.lenses.implicits._
import org.mimirdb.lenses.inference.InferTypes
import org.mimirdb.spark.SparkPrimitive.dataTypeFormat

object TypeInferenceLens
  extends Lens
{
  def train(input: DataFrame, rawConfig: JsValue): JsValue = 
  {
    Json.toJson(
      InferTypes(input)
        .map { field => field.name -> field.dataType }
        .toMap
    )
  }
  def create(input: DataFrame, config: JsValue, context: String): DataFrame = 
  {
    val mappedTypes = config.as[Map[String, DataType]]
    input.select(
      input.schema
           .fieldNames
           .map { attribute =>
              mappedTypes.get(attribute) match {
                case Some(t) => input(attribute).castWithCaveat(t, context)
                                                .as(attribute)
                case None => input(attribute)
              }
      }:_*
    )
  }
}