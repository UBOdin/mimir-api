package org.mimirdb.lenses.implementation

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.Lens
import org.mimirdb.spark.SparkPrimitive.dataTypeFormat
import org.mimirdb.rowids.AnnotateWithRowIds
import org.apache.spark.sql.Column

case class CommentLensConfig(
  comments: Seq[CommentParams]
)

object CommentLensConfig
{
  implicit val format:Format[CommentLensConfig] = Json.format

  def apply(comment:String): CommentLensConfig =
  {
    CommentLensConfig(
      Seq(CommentParams(
          comment,
          AnnotateWithRowIds.ATTRIBUTE,
          Seq(),
          None
        )
      )
    )
  }
}

case class CommentParams(
  comment: String,
  expr: String,
  rows: Seq[String],
  resultCol: Option[String]
)

object CommentParams
{
  implicit val format:Format[CommentParams] = Json.format
}

object CommentLens
  extends Lens
{
  def train(input: DataFrame, rawConfig: JsValue): JsValue = 
  {
    Json.toJson(
      rawConfig match {
        case JsString(key) => CommentLensConfig(key)
        case _:JsObject => rawConfig.as[CommentLensConfig]
        case _ => throw new IllegalArgumentException(s"Invalid CommentLens configuration: $rawConfig")
      }
    )
  }
  def create(input: DataFrame, rawConfig: JsValue, context: String): DataFrame = 
  {
    val config = rawConfig.as[CommentLensConfig]
    config.comments.foldLeft(AnnotateWithRowIds(input))((init, curr) => {
      val caveatCond = 
        if(curr.rows.isEmpty) 
          lit(true)
        else 
          curr.rows.foldLeft(lit(false))( (cond, row) => {
            cond.or(col(AnnotateWithRowIds.ATTRIBUTE).eqNullSafe(lit(row)))
          })
      val resultCol = curr.resultCol.getOrElse(s"COMMENT_${config.comments.indexOf(curr)}")
      val ccol = expr(curr.expr).caveatIf(curr.comment, caveatCond).as(resultCol)
      val fieldRefs = 
      (init.schema
           .fieldNames
           .map { field =>
              if(field.equalsIgnoreCase(resultCol)) { ccol }
              else { init(field).as(field) }
           }).toSeq ++ (curr.resultCol match {
             case Some(c) if init.schema.fieldNames.contains(c) => Seq(ccol) 
             case Some(c) => Seq[Column]()
             case None => Seq(ccol)
           })
      init.select(fieldRefs:_*)
    })
    
  }

}