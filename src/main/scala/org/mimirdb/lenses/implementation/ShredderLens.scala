package org.mimirdb.lenses.implementation

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ DataType, StringType }

import org.mimirdb.util.JsonUtils
import org.mimirdb.lenses.Lens
import org.mimirdb.lenses.implicits._


trait ShreddingOperation { def expr(input: Column): Column }

/**
 * Regular expression pattern matching.  Pick out the nth group in the pattern
 */
case class PatternField(regexp: String, group: Int = 1) extends ShreddingOperation
{ def expr(input: Column): Column = regexp_extract(input, regexp, group) }
object PatternField { implicit val format: Format[PatternField] = Json.format }

/**
 * Use a regular expression to split the field and pick the nth resulting field
 */
case class ExtractedField(separator: String, field: Int) extends ShreddingOperation
{ def expr(input: Column): Column = element_at(split(input, separator), field) }
object ExtractedField { implicit val format: Format[ExtractedField] = Json.format }

/**
 * Use a regular expression to split the field and create one row for each resulting value
 */
case class ExplodedField(separator: String) extends ShreddingOperation
{ def expr(input: Column): Column = explode(split(input, separator)) }
object ExplodedField { implicit val format: Format[ExplodedField] = Json.format }

/**
 * Pass through the field unchanged
 */
case class PassthroughField() extends ShreddingOperation
{ def expr(input: Column): Column = input }

/**
 * Strip out a substring of the specified field
 */
case class SubstringField(start: Int, end: Int) extends ShreddingOperation
{ def expr(input: Column): Column = substring(input, start, end - start) }
object SubstringField { implicit val format: Format[SubstringField] = Json.format }


object ShreddingOperation
{
  implicit val format:Format[ShreddingOperation] = Format[ShreddingOperation](
    new Reads[ShreddingOperation] { 
      def reads(j: JsValue): JsResult[ShreddingOperation] = 
        j.as[Map[String, JsValue]]
         .get("op")
         .getOrElse { return JsError() }
         .as[String] match {
          case "pattern" => JsSuccess(j.as[PatternField])
          case "field" => JsSuccess(j.as[ExtractedField])
          case "explode" => JsSuccess(j.as[ExplodedField])
          case "pass"    => JsSuccess(PassthroughField())
          case "substring" => JsSuccess(j.as[SubstringField])
        }
    },
    new Writes[ShreddingOperation] {
      def writes(o: ShreddingOperation): JsValue = 
      {
        val (j, name) = o match {
          case x:PatternField     => (Json.toJson(x), "pattern")
          case x:ExtractedField   => (Json.toJson(x), "field")
          case x:ExplodedField    => (Json.toJson(x), "explode")
          case x:PassthroughField => (Json.obj(),     "pass")
          case x:SubstringField   => (Json.toJson(x), "substring")
        }
        JsonUtils.addFieldsToObject(j, "op" -> Json.toJson(name))
      }
    }
  )
}


case class ShreddedColumn(
  input: String,
  output: String,
  operation: ShreddingOperation
)
object ShreddedColumn
{
  implicit val format:Format[ShreddedColumn] = Format[ShreddedColumn](
    new Reads[ShreddedColumn] { 
      def reads(j: JsValue): JsResult[ShreddedColumn] =
      {
        val m = j.as[Map[String,JsValue]]
        JsSuccess(ShreddedColumn(
          m.get("input").getOrElse { return JsError() }.as[String],
          m.get("output").getOrElse { return JsError() }.as[String],
          j.as[ShreddingOperation]
        ))
      }
    },
    new Writes[ShreddedColumn] {
      def writes(o: ShreddedColumn): JsValue = 
        JsonUtils.addFieldsToObject(Json.toJson(o.operation),
          "input" -> Json.toJson(o.input),
          "output" -> Json.toJson(o.output),
        )
    }
  )
}

case class ShredderLensConfig(
  keepOriginalColumns: Boolean = true,
  shreds: Seq[ShreddedColumn],
)
object ShredderLensConfig
{
  implicit val format:Format[ShredderLensConfig] = Json.format
}


object ShredderLens 
  extends Lens
{
  def train(input: DataFrame, rawConfig: JsValue): JsValue = 
  {
    val config = rawConfig.as[ShredderLensConfig]
    for(shred <- config.shreds) { assert(input(shred.input) != null) }
    Json.toJson(config)
  }
  def create(input: DataFrame, rawConfig: JsValue, context: String): DataFrame = 
  {
    val config = rawConfig.as[ShredderLensConfig]
    val outputs: Seq[Column] = 
      config.shreds.map { shred =>
        shred.operation.expr(input(shred.input))
          .as(shred.output)
      } ++ (if(config.keepOriginalColumns) { 
                input.schema.fieldNames.map { input(_) } 
              } else { Seq.empty })

    input.select(outputs:_*)
  }
}