package org.mimirdb.data

import play.api.libs.json._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{ SparkSession, DataFrame }

import org.mimirdb.lenses.Lenses

case class LoadConstructor(
  url: String,
  format: String,
  sparkOptions: Map[String, String],
  lenses: Seq[(String, JsValue, String)] = Seq()
)
  extends DataFrameConstructor
  with LazyLogging
{
  def construct(
    spark: SparkSession, 
    context: Map[String, DataFrame] = Map()
  ): DataFrame =
  {
    var parser = spark.read.format(format)
    for((option, value) <- sparkOptions){
      parser = parser.option(option, value)
    }
    logger.trace(s"Creating dataframe for $format file from $url")
    return lenses.foldLeft(parser.load(url)) {
      (df, lens) => Lenses(lens._1).create(df, lens._2, lens._3)
    }

  }

  def withLens(
    spark: SparkSession, 
    lens: String, 
    contextText: String,
    initialConfig: JsValue = JsNull
  ) =
  {
    LoadConstructor(
      url,
      format,
      sparkOptions,
      lenses :+ (
        lens, 
        Lenses(lens).train(construct(spark), initialConfig), 
        contextText
      )
    )
  }
}

object LoadConstructor
  extends DataFrameConstructorCodec
{
  implicit val format: Format[LoadConstructor] = Json.format
  def apply(v: JsValue): DataFrameConstructor = v.as[LoadConstructor]
}