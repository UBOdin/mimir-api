package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, SparkSession, AnalysisException }
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.catalyst.expressions.Expression

import org.mimirdb.api.{ Request, JsonResponse, MimirAPI, ErrorResponse, FormattedError }
import org.mimirdb.data.{ DataFrameConstructor, DataFrameConstructorCodec, DefaultProvenance }
import org.mimirdb.lenses.AnnotateImplicitHeuristics
import org.mimirdb.util.ErrorUtils
import org.mimirdb.spark.{ GetViewDependencies, Schema }
import org.mimirdb.spark.Schema.fieldFormat
import org.mimirdb.spark.InjectedSparkSQL
import com.typesafe.scalalogging.LazyLogging



case class CreateViewRequest (
            /* temporary view definitions for use in creating the view */
                  input: Map[String,String],
            /* names for functions stored in blob storage */
                  functions: Option[Map[String, String]],
            /* query for view */
                  query: String,
            /* optional name for the result table */
                  resultName: Option[String],
            /* optional properties */
                  properties: Option[Map[String,JsValue]]
)  
  extends Request 
  with DataFrameConstructor 
  with LazyLogging
  with DefaultProvenance
{

  lazy val output = 
    resultName.getOrElse {
      val lensNameBase = (input.toString + query).hashCode()
      "VIEW_" + (lensNameBase.toString().replace("-", ""))
    }

  def construct(spark: SparkSession, context: Map[String, () => DataFrame]): DataFrame =
  {
    var df = InjectedSparkSQL(spark)(
                  sqlText = query, 
                  tableMappings = input.mapValues { context(_) },
                  allowMappedTablesOnly = true,
                  functionMappings = 
                    functions.getOrElse { Map.empty }
                             .mapValues { blobID => 
                                { args => MimirAPI.blobs
                                                  .getPythonUDF(blobID)
                                                  .get
                                                  .apply(args) } 
                              } 
              )
    df = AnnotateImplicitHeuristics(df)
    return df 
  }

  def handle = {
    try {
      MimirAPI.catalog.put(
        output, 
        this,
        input.values.toSet,
        properties = properties.getOrElse { Map.empty }
      )
    } catch {
      case e:AnalysisException => {
        e.printStackTrace()
        val msg =  ErrorUtils.prettyAnalysisError(e, query)
        println(s"##############\n$msg\n##############")
        throw FormattedError(ErrorResponse(e,msg))
      }
    }
    val df = MimirAPI.catalog.get(output)
    CreateViewResponse(
      name = output,
      dependencies = GetViewDependencies(df).toSeq,
      functions = Seq(),
      schema = Schema(df),
      properties = Map.empty
    )
  }
}

object CreateViewRequest extends DataFrameConstructorCodec {
  implicit val format: Format[CreateViewRequest] = Json.format
  def apply(j: JsValue) = j.as[CreateViewRequest]
}

case class CreateViewResponse (
            /* name of resulting view */
            name: String,
            /* view dependencies (tables that this view reads from) */
            dependencies: Seq[String],
            /* function dependencies (functions that this view invoked) */
            functions: Seq[String],
            /* the schema of the resulting view */
            schema: Seq[StructField],
            /* Properties associated with the newly created view */
            properties: Map[String,JsValue]
) extends JsonResponse[CreateViewResponse]

object CreateViewResponse {
  implicit val format: Format[CreateViewResponse] = Json.format
}

