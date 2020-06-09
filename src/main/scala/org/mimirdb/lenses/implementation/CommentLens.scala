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
import java.sql.SQLException
import com.typesafe.scalalogging.LazyLogging

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
          None,
          None,
          None
        )
      )
    )
  }
}

case class CommentParams(
  comment: String,
  target: Option[String],
  rows: Option[Seq[String]],
  condition: Option[String]
)
{
  def selectedRows: Column =
  {
    if(!condition.isEmpty){
      expr(condition.get)
    } else if(!rows.toSeq.flatten.isEmpty) {
      rows.toSeq.flatten.foldLeft(lit(false))( (cond, row) => {
        cond.or(col(AnnotateWithRowIds.ATTRIBUTE).eqNullSafe(lit(row)))
      })
    } else { 
      lit(true)
    }
  }
}

object CommentParams
{
  implicit val format:Format[CommentParams] = Json.format
}

object CommentLens
  extends Lens
  with LazyLogging
{
  def train(input: DataFrame, rawConfig: JsValue): JsValue = 
  {
    val config =
      rawConfig match {
        case JsString(key) => CommentLensConfig(key)
        case _:JsObject => rawConfig.as[CommentLensConfig]
        case _ => throw new IllegalArgumentException(s"Invalid CommentLens configuration: $rawConfig")
      }
    for(comment <- config.comments){
      for(target <- comment.target){
        if(input.schema.fieldNames.find { _.equalsIgnoreCase(target) }.isEmpty) {
          throw new SQLException(s"Invalid comment column: ${comment.target} (out of ${input.schema.fieldNames.mkString(", ")})")
        }
      }
    }
    Json.toJson(config)
  }
  def create(input: DataFrame, rawConfig: JsValue, context: String): DataFrame = 
  {
    val config = rawConfig.as[CommentLensConfig]
    
    AnnotateWithRowIds.withRowId(input) { df => 
      var rowComments:List[CommentParams] = Nil
      var cellComments:List[(String, String, Column)] = Nil

      for(comment <- config.comments){
        comment.target match {
          case Some(target) => 
            cellComments = (
              target.toLowerCase, 
              comment.comment, 
              comment.selectedRows
            ) :: cellComments
          case None => 
            rowComments = comment :: rowComments
        }
      }

      val cellCommentMap = cellComments.groupBy { _._1 }

      val outputs = 
        df.schema
          .fieldNames
          .map { field =>
            cellCommentMap.getOrElse(field.toLowerCase, Seq())
              .foldLeft(df(field)) { case (column, (_, comment, rowSelection)) =>
                column.caveatIf(comment, rowSelection)
              }.as(field)
          }

      rowComments.foldLeft(
        df.select( outputs:_* )
      ) { (df, comment) => 
        df.caveatIf(comment.comment, comment.selectedRows)
      }
    }    
  }

}