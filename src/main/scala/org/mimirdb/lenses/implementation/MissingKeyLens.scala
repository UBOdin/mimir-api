package org.mimirdb.lenses.implementation

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.Lens
import org.mimirdb.spark.SparkPrimitive.dataTypeFormat

case class MissingKeyLensConfig(
  key: String,
  t: DataType,
  low: Long,
  high: Long,
  step: Long
)

object MissingKeyLensConfig
{
  implicit val format:Format[MissingKeyLensConfig] = Json.format

  def apply(key: String, df: DataFrame): MissingKeyLensConfig =
  {
    val t = df.schema(key).dataType

    val stats = df.select(
      min(df(key).cast("long")),
      max(df(key).cast("long")),
      lit(1l)
    ).collect()(0)

    MissingKeyLensConfig(
      key,
      t,
      stats.getAs[Long](0),
      stats.getAs[Long](1)+1, // Add 1 to make the high value exclusive
      stats.getAs[Long](2)
    )
  }
}

object MissingKeyLens
  extends Lens
{
  def train(input: DataFrame, rawConfig: JsValue): JsValue = 
  {
    Json.toJson(
      rawConfig match {
        case JsString(key) => MissingKeyLensConfig(key, input)
        case _:JsObject => rawConfig.as[MissingKeyLensConfig]
        case _ => throw new IllegalArgumentException(s"Invalid MissingKeyLens configuration: $rawConfig")
      }
    )
  }
  def create(input: DataFrame, rawConfig: JsValue, context: String): DataFrame = 
  {
    val config = rawConfig.as[MissingKeyLensConfig]
    val range = input.queryExecution
                     .sparkSession
                     .range(config.low, config.high, config.step)
    val idField = range("id")
    val fieldRefs = 
      input.schema
           .fieldNames
           .map { field =>
              // Since the key field may be missing values, use the "id" field
              // from the range as a canonical version of it.
              if(field.equalsIgnoreCase(config.key)) { idField.as(field) }
              // All other fields get pushed through untouched (though we)
              // drop the "_2" from the join.
              else { input(field).as(field) }
           }

    // A left-outer join enforces the presence of all key values.
    range.join(
        input,
        range("id") === input(config.key),
        "left_outer"
      )
    // Register a caveat for any record that doesn't have a match
      .caveatIf(
        concat(lit("A missing key ("), idField, lit(s") in $context was added")),
        input(config.key).isNull
      )
    // And project out the new "id" field.
      .select(fieldRefs:_*)
  }

}