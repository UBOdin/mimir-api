package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.SparkSession

import org.mimirdb.api.{ Request, Response }
import org.mimirdb.api.MimirAPI

case class CreateViewRequest (
            /* temporary view definitions for use in creating the view */
                  input: Map[String,String],
            /* query for view */
                  query: String,
            /* optional name for the result table */
                  resultName: Option[String]
)  extends Request {

  lazy val output = 
    resultName.getOrElse {
      val lensNameBase = (input.toString + query).hashCode()
      "VIEW_" + (lensNameBase.toString().replace("-", ""))
    }

  def handle = {
    create(
      query,
      output, 
      input
    )
    Json.toJson(CreateViewResponse(output))
  }
  def create(
    query: String,
    targetTable: String,
    temporaryTables: Map[String,String] = Map(),
    sparkSession: SparkSession = MimirAPI.sparkSession
  ){ 
    ???
  }
}

object CreateViewRequest {
  implicit val format: Format[CreateViewRequest] = Json.format
}

case class CreateViewResponse (
            /* name of resulting view */
                  viewName: String
) extends Response

object CreateViewResponse {
  implicit val format: Format[CreateViewResponse] = Json.format
}

