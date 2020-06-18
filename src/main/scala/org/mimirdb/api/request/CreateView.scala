package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.AnalysisException

import org.mimirdb.api.{ Request, Response, MimirAPI, ErrorResponse, FormattedError, Schema }
import org.mimirdb.data.{ DataFrameConstructor, DataFrameConstructorCodec }
import org.mimirdb.lenses.AnnotateImplicitHeuristics
import org.mimirdb.util.ErrorUtils
import org.mimirdb.spark.GetViewDependencies



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
      context(internalName).createOrReplaceTempView(userFacingName)
    }
    var df = spark.sql(query)
    df = AnnotateImplicitHeuristics(df)
    return df 
  }

  def handle = {
    try {
      MimirAPI.catalog.put(
        output, 
        this,
        input.values.toSet
      )
    } catch {
      case e:AnalysisException => {
        val msg =  ErrorUtils.prettyAnalysisEror(e, query)
        println(s"##############\n$msg\n##############")
        throw FormattedError(ErrorResponse(e,msg))
      }
    }
    val df = MimirAPI.catalog.get(output)
    Json.toJson(CreateViewResponse(
      output,
      GetViewDependencies(df).toSeq,
      Schema(df)
    ))
  }
}

object CreateViewRequest extends DataFrameConstructorCodec {
  implicit val format: Format[CreateViewRequest] = Json.format
  def apply(j: JsValue) = j.as[CreateViewRequest]
}

case class CreateViewResponse (
            /* name of resulting view */
            viewName: String,
            /* view dependencies (tables that this view reads from) */
            dependencies: Seq[String],
            /* the schema of the resulting view */
            schema: Seq[Schema]
) extends Response

object CreateViewResponse {
  implicit val format: Format[CreateViewResponse] = Json.format
}

