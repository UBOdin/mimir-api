package org.mimirdb.data

import play.api.libs.json._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{ SparkSession, DataFrame, Dataset }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrameReader
import org.apache.spark.sql.execution.datasources.csv.TextInputCSVDataSource
import org.mimirdb.spark.Schema.fieldFormat

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
  contextText: Option[String] = None,
  proposedSchema: Option[Seq[StructField]] = None
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

  /**
   * Merge the proposed schema with the actual schema obtained from the
   * data loader to obtain the actual schema we should emit.
   *
   * If the proposed schema is too short, fields off the end will use 
   * their default names.
   * 
   * If the proposed schema is too long, it will be truncated.
   */
  def actualizeProposedSchema(currSchema: Seq[StructField]): Seq[StructField] =
  {
    proposedSchema match { 
      case None => return currSchema
      case Some(realProposedSchema) => 
        val targetSchemaFields: Seq[StructField] = 
          if(realProposedSchema.size > currSchema.size){
            realProposedSchema.take(currSchema.size)
          } else if(realProposedSchema.size < currSchema.size){
            realProposedSchema ++ currSchema.toSeq.drop(realProposedSchema.size)
          } else {
            realProposedSchema
          }
        assert(currSchema.size == targetSchemaFields.size)
        return targetSchemaFields
    }
  }

  def convertToProposedSchema(df: DataFrame): DataFrame =
  {
    if(proposedSchema.isEmpty || proposedSchema.get.equals(df.schema.fields.toSeq)){
      return df
    } else {
      val currSchema = df.schema.fields
      val targetSchemaFields = actualizeProposedSchema(currSchema)

      df.select(
        (targetSchemaFields.zip(currSchema).map { 
          case (target, curr) => 
            if(target.dataType.equals(curr.dataType)){
              if(target.name.equals(curr.name)){
                df(curr.name)
              } else {
                df(curr.name).as(target.name)
              }
            } else {
              df(curr.name).cast(target.dataType).as(target.name)
            }
      }):_*)
    }

  }

  def loadWithoutCaveats(spark: SparkSession): DataFrame = 
  {
    var parser = spark.read.format(format)
    for((option, value) <- sparkOptions){
      parser = parser.option(option, value)
    }
    var df = parser.load(url)
    df = convertToProposedSchema(df)
    return df
  }

  val LEADING_WHITESPACE = raw"^[ \t\n\r]+"
  val INVALID_LEADING_CHARS = raw"^[^a-zA-Z_]+"
  val INVALID_INNER_CHARS = raw"[^a-zA-Z0-9_]+"

  def cleanColumnName(name: String): String =
    name.replaceAll(LEADING_WHITESPACE, "")
        .replaceAll(INVALID_LEADING_CHARS, "_")
        .replaceAll(INVALID_INNER_CHARS, "_")



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
      StructType(
        actualizeProposedSchema(
          TextInputCSVDataSource.inferFromDataset(
            spark, 
            data.map { _.getAs[String](0) },
            data.take(1).headOption.map { _.getAs[String](0) },
            options
          ).map { column => 
            StructField(cleanColumnName(column.name), column.dataType)
          }
        )
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
        // when(col("value").isNull, null)
        // .otherwise(
          from_csv( col("value"), annotatedSchema, extraOptions ++ sparkOptions )
        // ) 
      as "csv"
      ).caveatIf(
        concat(
          lit("Error Loading Row: '"), 
          col("raw"), 
          lit(s"'${contextText.map { " (in "+_+")" }.getOrElse {""}}")
        ),
        col("csv").isNull or not(col(s"csv.$ERROR_COL").isNull)
      ).select(
        baseSchema.fields.map { field => 
          // when(col("csv").isNull, null)
            // .otherwise(
              col("csv").getField(field.name)
            // )
             .as(field.name)
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
      Some(contextText),
      proposedSchema
    )
  }
}

object LoadConstructor
  extends DataFrameConstructorCodec
{
  implicit val format: Format[LoadConstructor] = Json.format
  def apply(v: JsValue): DataFrameConstructor = v.as[LoadConstructor]

}