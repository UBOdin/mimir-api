package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, SparkSession }

import org.mimirdb.api.{ Request, Response, MimirAPI }
import org.mimirdb.data.{ DataFrameConstructor, DataFrameConstructorCodec }

case class CreateViewRequest (
            /* temporary view definitions for use in creating the view */
                  input: Map[String,String],
            /* query for view */
                  query: String,
            /* optional name for the result table */
                  resultName: Option[String]
)  extends Request with DataFrameConstructor {

  lazy val output = 
    resultName.getOrElse {
      val lensNameBase = (input.toString + query).hashCode()
      "VIEW_" + (lensNameBase.toString().replace("-", ""))
    }

  def construct(spark: SparkSession, context: Map[String,DataFrame]): DataFrame =
  {
    // Create temp views so that we can reference mimir tables by name
    // TODO: ensure that this isn't a race condition!
    for((userFacingName, internalName) <- input){
      context(internalName).createTempView(userFacingName)
    }
    spark.sql(query)
  }

  def handle = {
    MimirAPI.catalog.put(
      output, 
      this,
      input.values.toSet
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

object CreateViewRequest extends DataFrameConstructorCodec {
  implicit val format: Format[CreateViewRequest] = Json.format
  def apply(j: JsValue) = j.as[CreateViewRequest]
}

case class CreateViewResponse (
            /* name of resulting view */
                  viewName: String
) extends Response

object CreateViewResponse {
  implicit val format: Format[CreateViewResponse] = Json.format
}

