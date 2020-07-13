package org.mimirdb.data

import play.api.libs.json._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.execution.datasources.csv.TextInputCSVDataSource

import org.mimirdb.lenses.Lenses
import org.mimirdb.caveats.implicits._
import org.apache.spark.sql.TypedColumn
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.csv.CSVHeaderChecker
import org.apache.spark.sql.execution.datasources.csv.CSVUtils
import org.mimirdb.rowids.AnnotateWithSequenceNumber

case class LoadConstructor(
  url: String,
  format: String,
  sparkOptions: Map[String, String],
  lenses: Seq[(String, JsValue, String)] = Seq(),
  contextText: Option[String] = None
) 
  extends DataFrameConstructor
  with LazyLogging
{
  def construct(
    spark: SparkSession, 
    context: Map[String, DataFrame] = Map()
  ): DataFrame =
  {
    var df =
      format match {
        case FileFormat.CSV => loadCSVWithCaveats(spark)
        case _ => loadWithoutCaveats(spark)
      }

    df = lenses.foldLeft(df) {
      (df, lens) => Lenses(lens._1).create(df, lens._2, lens._3)
    }

    return df
  }

  def loadWithoutCaveats(spark: SparkSession): DataFrame = 
  {
    var parser = spark.read.format(format)
    for((option, value) <- sparkOptions){
      parser = parser.option(option, value)
    }
    parser.load(url)
  }

  def loadCSVWithCaveats(spark: SparkSession): DataFrame =
  {
    // based largely on Apache Spark's DataFrameReader's csv(Dataset[String]) method

    import spark.implicits._
    val ERROR_COL = "__MIMIR_CSV_LOAD_ERROR"
    val extraOptions = Map(
      "mode" -> "PERMISSIVE",
      "columnNameOfCorruptRecord" -> ERROR_COL
    )
    val options = 
      new CSVOptions(
        extraOptions ++ sparkOptions,
        spark.sessionState.conf.csvColumnPruning,
        spark.sessionState.conf.sessionLocalTimeZone
      )
    val data: DataFrame = 
      spark.read
           .format("text")
           .load(url)
    
    val maybeFirstLine = 
      if(options.headerFlag){
        data.take(1).headOption.map { _.getAs[String](0) }
      } else { None }

    val baseSchema = 
      TextInputCSVDataSource.inferFromDataset(
        spark, 
        data.map { _.getAs[String](0) },
        data.take(1).headOption.map { _.getAs[String](0) },
        options
      )

    val annotatedSchema = 
      baseSchema.add(ERROR_COL, StringType)

    val dataWithoutHeader = 
      maybeFirstLine.map { firstLine => 
        val headerChecker = new CSVHeaderChecker(
          baseSchema,
          options,
          source = contextText.getOrElse { "CSV File" }
        )
        headerChecker.checkHeaderColumnNames(firstLine)
        AnnotateWithSequenceNumber.withSequenceNumber(data) { 
          _.filter(col(AnnotateWithSequenceNumber.ATTRIBUTE) =!= 
                    AnnotateWithSequenceNumber.DEFAULT_FIRST_ROW)
        }
      }.getOrElse { data }

    dataWithoutHeader
      .select(
        col("value") as "raw",
        from_csv( col("value"), annotatedSchema, extraOptions ++ sparkOptions ) as "csv"
      ).caveatIf(
        concat(
          lit("Error Loading Row: '"), 
          col("raw"), 
          lit(s"'${contextText.map { " (in "+_+")" }.getOrElse {""}}")
        ),
        not(col(s"csv.$ERROR_COL").isNull)
      ).select(
        baseSchema.fields.map { field => 
          col("csv").getField(field.name) as field.name
        }:_*
      )
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
      ),
      Some(contextText)
    )
  }
}

object LoadConstructor
  extends DataFrameConstructorCodec
{
  implicit val format: Format[LoadConstructor] = Json.format
  def apply(v: JsValue): DataFrameConstructor = v.as[LoadConstructor]

}