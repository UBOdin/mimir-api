package org.mimirdb.lenses.implementation

import play.api.libs.json._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.Lens


object LoadErrorTrackingLens
  extends Lens
{
  val DATASOURCE_IS_ERROR_COLUMN     = "_MIMIR_DATASOURCE_IS_ERROR"
  val DATASOURCE_ERROR_ROW_COLUMN    = "_MIMIR_DATASOURCE_ERROR_ROW"

  def train(input: DataFrame, rawConfig: JsValue): JsValue = rawConfig
  def create(input: DataFrame, config: JsValue, context: String): DataFrame = 
  {
    val fields = input.schema.fieldNames.filter { field => 
                    !field.equals(DATASOURCE_IS_ERROR_COLUMN) &&
                    !field.equals(DATASOURCE_ERROR_ROW_COLUMN)
                  }

    input.caveatIf(
      concat(
        lit("Error parsing row: "),
        col(DATASOURCE_ERROR_ROW_COLUMN)
      ),
      input(DATASOURCE_IS_ERROR_COLUMN)
    ).select(
      fields.map { input(_) }:_*
    )
  }

}