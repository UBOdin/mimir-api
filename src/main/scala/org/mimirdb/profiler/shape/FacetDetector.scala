package org.mimirdb.profiler.shape

import play.api.libs.json._
import org.apache.spark.sql.DataFrame

trait FacetDetector
{
  def identity: String
  def apply(query: DataFrame): Seq[Facet]
  def decode(facet: JsValue)(): Facet
}