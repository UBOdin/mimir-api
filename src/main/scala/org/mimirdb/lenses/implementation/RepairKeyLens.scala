package org.mimirdb.lenses.implementation

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.aggregate.MaxBy
import org.apache.spark.sql.functions._

import org.mimirdb.lenses.Lens
import org.mimirdb.caveats.implicits._

case class RepairKeyLensConfig(
  key: String,
  weight: Option[String] = None
)

object RepairKeyLensConfig
{
  implicit val format:Format[RepairKeyLensConfig] = Json.format
}

object RepairKeyLens
  extends Lens
{
  def COUNT_COLUMN(x: String) = "__MIMIR_REPAIR_KEY_COUNT_"+x

  def train(input: DataFrame, rawConfig: JsValue): JsValue = 
  {
    val config = rawConfig.as[RepairKeyLensConfig]
    // make sure the attributes exist
    input(config.key)
    config.weight match { 
      case Some(weight) => input(weight)
      case None => ()
    }
    Json.toJson(config)
  }
  def create(input: DataFrame, rawConfig: JsValue, context: String): DataFrame = 
  {
    val config = rawConfig.as[RepairKeyLensConfig]
    val keyAttribute = input(config.key)
    val nonKeyAttributes = 
        input.schema.fieldNames
             .filter { !_.equalsIgnoreCase(config.key) }

    val nonKeyCounts:Seq[Column] =
        nonKeyAttributes
          .map { attr => countDistinct(input(attr)) as COUNT_COLUMN(attr) }

    val nonKeyAggregates:Seq[Column] = 
      config.weight.map { input(_) } match {
        case None => {
          nonKeyAttributes
            .map { attr => first(input(attr)) as attr } 
        }
        case Some(weight) => {
          nonKeyAttributes
            .map { attr => 
              new Column(
                MaxBy(input(attr).expr, weight.expr)
                  .toAggregateExpression(false)
              ).as(attr)
            }
        }
      }

    val output = 
      input.groupBy(keyAttribute)
           .agg( nonKeyAggregates.head, (nonKeyAggregates.tail ++ nonKeyCounts) :_* )
    
    val outputKeyAttribute = output(config.key)

    val outputSchema = 
      input.schema.fieldNames
           .map { 
              case field if field.equalsIgnoreCase(config.key) => output(field)
              case field => {
                output(field).caveatIf(
                  concat(
                    output(COUNT_COLUMN(field)).cast(StringType),
                    lit(s" different values for $context.$field for $context.${config.key} = "),
                    outputKeyAttribute.cast(StringType)
                  ),
                  output(COUNT_COLUMN(field)) > 1
                ).as(field)
              }
           }
    output.select(outputSchema:_*)
  }
}
