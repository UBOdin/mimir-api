package org.mimirdb.lenses.implementation

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.Lens
import org.mimirdb.spark.SparkPrimitive.dataTypeFormat
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.Column

case class MissingValueLensConfig(
  colsStrategy: Seq[(String,String)]
)

object MissingValueLensConfig
{
  implicit val format:Format[MissingValueLensConfig] = Json.format

  def apply(cols: Seq[String], df: DataFrame): MissingValueLensConfig =
  {
    val colsStrategy = cols.map(mvcol => {
      val t = df.schema(mvcol).dataType
      t match {
        //TODO: implement imputers for types 
        case StringType => (mvcol, "not_implemented_yet")
        case nt:NumericType => (mvcol, "mean")
      }
      
    })

    MissingValueLensConfig(
      colsStrategy
    )
  }
  
}

object MissingValueLens
  extends Lens
{
  def train(input: DataFrame, rawConfig: JsValue): JsValue = 
  {
    Json.toJson(
      rawConfig match {
        case JsArray(cols) => MissingValueLensConfig(cols.map(_.as[String]), input)
        case _:JsObject => rawConfig.as[MissingValueLensConfig]
        case _ => throw new IllegalArgumentException(s"Invalid MissingValueLens configuration: $rawConfig")
      }
    )
  }
  def create(input: DataFrame, rawConfig: JsValue, context: String): DataFrame = 
  {
    val config = rawConfig.as[MissingValueLensConfig]
    val fieldNames = input.schema.fieldNames
    config.colsStrategy.foldLeft(input)((inputdf, colStrategy) => {
      val (imputeCol, strategy) = colStrategy
      //TODO: use imputer that we will implement for strategy
      val imputer = new Imputer().
        setStrategy(strategy).
        setMissingValue(0).
        setInputCols(Array(imputeCol)).setOutputCols(Array(imputeCol));
      val fieldRef = inputdf(imputeCol)
      val model = imputer.fit(inputdf.filter(fieldRef.isNotNull))//.sample(.8))
      model.transform(inputdf.caveatIf(s"$imputeCol was null and we imputed it with $strategy", fieldRef.isNull));
    }).select(fieldNames.map(col(_)):_*)
  }

}