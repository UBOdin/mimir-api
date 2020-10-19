package org.mimirdb.data

import play.api.libs.json._
import scala.collection.JavaConverters
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{ SparkSession, DataFrame, Row }
import org.apache.spark.sql.types._

import org.mimirdb.spark.{ SparkPrimitive, Schema }
import org.mimirdb.spark.Schema.fieldFormat

case class InlineConstructor(
  schema: Seq[StructField],
  data: Seq[Seq[JsValue]]
) extends DataFrameConstructor
  with DefaultProvenance
{
  def construct(
    spark: SparkSession, 
    context: Map[String, () => DataFrame] = Map()
  ): DataFrame =
  {
    val types = schema.map { _.dataType }
    val rows:Seq[Row] = 
      data.map { row => 
        Row.fromSeq(row.zip(types).map { case (field, t) => 
          SparkPrimitive.decode(field, t, castStrings = true)
        })
      }
    return spark.createDataFrame(
      JavaConverters.seqAsJavaList(rows),
      StructType(schema)
    )
  }


}


object InlineConstructor
  extends DataFrameConstructorCodec
{
  implicit val format: Format[InlineConstructor] = Json.format
  def apply(v: JsValue): DataFrameConstructor = v.as[InlineConstructor]
}
