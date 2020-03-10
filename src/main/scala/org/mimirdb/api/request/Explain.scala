package org.mimirdb.api.request


import play.api.libs.json._
import org.apache.spark.sql.SparkSession

import org.mimirdb.api.{ Request, Response }
import org.mimirdb.api.MimirAPI
import org.mimirdb.caveats.{ Caveat, CaveatSet }
import org.mimirdb.api.CaveatFormat._


case class ExplainCellRequest (
            /* query to explain */
                  query: String,
            /* rowid of cell */
                  row: String,
            /* column of cell */
                  col: String
) extends Request {
  def handle = Json.toJson(ExplainResponse(Explain(
    query,
    rows = Seq(row),
    cols = Seq(col)
  )))

}

object ExplainCellRequest {
  implicit val format: Format[ExplainCellRequest] = Json.format
}


case class ExplainEverythingRequest (
            /* query to explain */
                  query: String
) extends Request {
  def handle = Json.toJson(ExplainResponse(Explain(
    query
  )))
}

object ExplainEverythingRequest {
  implicit val format: Format[ExplainEverythingRequest] = Json.format
}



case class ExplainResponse (
                  reasons: Seq[Caveat]
) extends Response

object ExplainResponse {
  implicit val format: Format[ExplainResponse] = Json.format
}


object Explain
{
  def apply(
    query: String, 
    rows: Seq[String] = null,
    cols: Seq[String] = null,
    schemaCaveats: Boolean = true,
    reasonCap: Int = 3,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): Seq[Caveat] = ???

  def coarsely(
    query: String, 
    rows: Seq[String] = null,
    cols: Seq[String] = null,
    schemaCaveats: Boolean = true,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): Seq[CaveatSet] = ???
}