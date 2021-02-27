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
    val givens: Map[String, DataType] = 
      rawConfig.asOpt[Map[String, DataType]]
               .getOrElse { Map.empty }

    val isGiven = givens.keySet

    val (stringCols, nonStringCols) = 
      input.schema
           .fields
           .filter { col => !isGiven(col.name) }
           .partition { _.dataType.equals(StringType) }

    val targets: Set[String] = 
      stringCols.map { _.name }.toSet -- givens.keySet

    println(s"Targets: $targets")

    Json.toJson(
      InferTypes(input, attributes = targets.toSeq)
        .map { field => field.name -> field.dataType }
        .toMap ++ givens ++ nonStringCols.map { col => col.name -> col.dataType }
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