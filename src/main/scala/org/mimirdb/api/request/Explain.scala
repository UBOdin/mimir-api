package org.mimirdb.api.request


import play.api.libs.json._
import org.apache.spark.sql.SparkSession

import org.mimirdb.api.{ Request, JsonResponse }
import org.mimirdb.api.MimirAPI
import org.mimirdb.caveats.{ Caveat, CaveatSet }
import org.mimirdb.api.CaveatFormat._
import org.mimirdb.caveats.implicits._
import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimirdb.spark.InjectedSparkSQL
import com.typesafe.scalalogging.LazyLogging

case class ExplainCellRequest (
            /* query to explain */
                  query: String,
            /* rowid of cell */
                  row: String,
            /* column of cell */
                  col: String
) extends Request {
  def handle = ExplainResponse(Explain(
    query,
    rows = Seq(row),
    cols = Seq(col),
    reasonCap = 20
  ))

}

object ExplainCellRequest {
  implicit val format: Format[ExplainCellRequest] = Json.format
}


case class ExplainEverythingRequest (
            /* query to explain */
                  query: String
) extends Request {
  def handle = ExplainResponse(Explain(
    query
  ))
}

object ExplainEverythingRequest {
  implicit val format: Format[ExplainEverythingRequest] = Json.format
}



case class ExplainResponse (
                  reasons: Seq[Caveat]
) extends JsonResponse[ExplainResponse]

object ExplainResponse {
  implicit val format: Format[ExplainResponse] = Json.format
}


object Explain
  extends LazyLogging
{
  def apply(
    query: String, 
    rows: Seq[String] = null,
    cols: Seq[String] = null,
    schemaCaveats: Boolean = true,
    reasonCap: Int = 3,
    spark: SparkSession = MimirAPI.sparkSession
  ): Seq[Caveat] =
  {
    val caveatSets = coarsely(query, rows, cols, schemaCaveats, spark)
    caveatSets.par
               .flatMap { caveatSet =>
                  val caveats = caveatSet.take(spark, reasonCap+1)
                  logger.trace(s"Expanding CaveatSet: \n${caveatSet}")
                  if(caveats.size > reasonCap){
                    caveats.slice(0, reasonCap) :+
                      Caveat(
                        s"... and ${caveatSet.size(spark) - reasonCap} more like the last",
                        None,
                        Seq()
                      )
                  } else {
                    caveats
                  }
               }
               .seq
  }

  def coarsely(
    query: String, 
    rows: Seq[String] = null,
    cols: Seq[String] = null,
    schemaCaveats: Boolean = true,
    spark: SparkSession = MimirAPI.sparkSession
  ): Seq[CaveatSet] = 
  {
    var df = InjectedSparkSQL(spark)(query, MimirAPI.catalog.allTableConstructors)
    val selectedCols = 
      Option(cols).getOrElse { df.schema.fieldNames.toSeq }.toSet
    if(rows != null){
      df = AnnotateWithRowIds(df)
      // println(s"EXPLAIN PLAN: \n${df.queryExecution.logical}")
      df = df.filter { df(AnnotateWithRowIds.ATTRIBUTE).isin(rows:_*) }
    }
    df.listCaveatSets(row = true, attributes = selectedCols)
  }
}