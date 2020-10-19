package org.mimirdb.data

import play.api.libs.json._
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.types.{ DataType, StructField }
import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimirdb.lenses.AnnotateImplicitHeuristics
import org.mimirdb.caveats.implicits._
import org.mimirdb.spark.Schema.fieldFormat

case class MaterializeConstructor(
  input: String,
  schema: Seq[StructField], 
  url: String,
  format: String, 
  options: Map[String,String]
)
  extends DataFrameConstructor
{

  def construct(
    spark: SparkSession, 
    context: Map[String,() => DataFrame]
  ): DataFrame = 
  {
    var parser = spark.read.format(format)
    for((option, value) <- options){
      parser = parser.option(option, value)
    }
    var df = parser.load(url)

    println(url)

    // add a silent projection to "strip out" all of the support metadata.
    df = df.select( schema.map { field => df(field.name) }:_* )
    
    return df
  }

  def provenance(
    spark: SparkSession, 
    context: Map[String,() => DataFrame]
  ): DataFrame = 
    context(input)()
}

object MaterializeConstructor
  extends DataFrameConstructorCodec
{
  implicit val format: Format[MaterializeConstructor] = Json.format
  def apply(v: JsValue): DataFrameConstructor = v.as[MaterializeConstructor]

  val DEFAULT_FORMAT = "parquet"

  def apply(input: String, catalog: Catalog): MaterializeConstructor = 
  {
    val format = DEFAULT_FORMAT
    var df = catalog.get(input)
    val schema = df.schema.fields.toSeq

    df = AnnotateImplicitHeuristics(df)
    df = AnnotateWithRowIds(df)
    df = df.trackCaveats.stripCaveats

    val url = catalog.staging.stage(df, format, Some("materialized"))
    MaterializeConstructor(input, schema, url, format, Map())
  }


}