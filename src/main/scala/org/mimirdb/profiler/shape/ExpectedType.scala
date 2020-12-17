package org.mimirdb.profiler.shape

import play.api.libs.json._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import org.mimirdb.util.StringUtils
import org.mimirdb.spark.Schema.fieldFormat

case class ExpectedType(field: StructField)
  extends Facet
{
  def identity = ExpectedType.identity
  def description = s"The column ${field.name} should be ${StringUtils.withDefiniteArticle(StringUtils.friendlyTypeString(field.dataType))}"
  def test(query:DataFrame): Seq[String] =
  {
    query.schema
         .fields
         .find { _.name.equalsIgnoreCase(field.name) }
         // silently pass through missing columns.  Should be caught by ExpectedColumns
         .flatMap { 
           case actual => 
             if( ! field.dataType.equals(actual.dataType) ) { 
               Some(s"${actual.name} is ${StringUtils.withDefiniteArticle(StringUtils.friendlyTypeString(actual.dataType))} (Expected ${StringUtils.withDefiniteArticle(StringUtils.friendlyTypeString(field.dataType))})") 
             } else { None }
         }
         .toSeq
  }
  def toJson = Json.toJson(this)
  def affectsColumn = Some(field.name)
}

object ExpectedType
  extends FacetDetector
{
  def identity = "ExpectedType"
  implicit val format: Format[ExpectedType] = Json.format
  def apply(query:DataFrame): Seq[Facet] = 
    query.schema.fields.map { ExpectedType(_) }.toSeq
  def decode(facet: JsValue)(): Facet = facet.as[ExpectedType]
}