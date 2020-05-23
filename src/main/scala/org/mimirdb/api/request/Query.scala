package org.mimirdb.api.request

import org.apache.spark.sql.{ SparkSession, DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.execution.{ ExtendedMode => SelectedExplainMode }
import org.apache.spark.sql.SparkSession

import play.api.libs.json._

import org.mimirdb.api.{ Request, Response, Schema }
import org.mimirdb.api.MimirAPI
import org.mimirdb.caveats.{ Caveat, CaveatSet }
import org.mimirdb.caveats.implicits._
import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimirdb.caveats.{ Constants => Caveats }
import org.mimirdb.spark.SparkPrimitive
import org.mimirdb.api.CaveatFormat._
import org.mimirdb.util.TimerUtils

import com.typesafe.scalalogging.LazyLogging

case class QueryMimirRequest (
            /* input for query */
                  input: String,
            /* query string - sql */
                  query: String,
            /* include taint in response */
                  includeUncertainty: Boolean,
            /* include reasons in response */
                  includeReasons: Boolean
) extends Request {
  def handle = {
    val inputSubstitutionQuery = query.replaceAll("\\{\\{\\s*input\\s*\\}\\}", input.toString) 
    if(includeReasons) {
      throw new UnsupportedOperationException("IncludeReasons is no longer supported")
    }
    Json.toJson(Query(
      inputSubstitutionQuery,
      includeUncertainty
    ))
  }
}

object QueryMimirRequest {
  implicit val format: Format[QueryMimirRequest] = Json.format
}


case class SchemaForQueryRequest (
            /* query string to get schema for - sql */
                  query: String
) extends Request {
  def handle = Json.toJson(SchemaList(Query.getSchema(query)))
}

object SchemaForQueryRequest {
implicit val format: Format[SchemaForQueryRequest] = Json.format
}

case class DataContainer (
                  schema: Seq[Schema],
                  data: Seq[Seq[Any]],
                  prov: Seq[String],
                  colTaint: Seq[Seq[Boolean]],
                  rowTaint: Seq[Boolean],
                  reasons: Seq[Seq[Caveat]]
) extends Response

object DataContainer {
  implicit val format: Format[DataContainer] = Format(
    new Reads[DataContainer] {
      def reads(data: JsValue): JsResult[DataContainer] =
      {
        val parsed = data.as[Map[String,JsValue]]
        val schema = parsed("schema").as[Seq[Schema]]
        val sparkSchema = schema.map { _.sparkType }
        JsSuccess(
          DataContainer(
            schema,
            parsed("data").as[Seq[Seq[JsValue]]].map { 
              _.zip(sparkSchema).map { case (dat, sch) => SparkPrimitive.decode(_, sch) } },
            parsed("prov").as[Seq[String]],
            parsed("colTaint").as[Seq[Seq[Boolean]]],
            parsed("rowTaint").as[Seq[Boolean]],
            parsed("reasons").as[Seq[Seq[Caveat]]]
          )
        )
      }
    },
    new Writes[DataContainer] { 
      def writes(data: DataContainer): JsValue = {
        val sparkSchema = data.schema.map { _.sparkType }
        Json.obj(
          "schema" -> data.schema,
          "data" -> data.data.map { row => 
            row.zip(sparkSchema).map { case (dat, sch) => 
              SparkPrimitive.encode(dat, sch) 
            }
          },
          "prov" -> data.prov,
          "colTaint" -> data.colTaint,
          "rowTaint" -> data.rowTaint,
          "reasons" -> data.reasons
        )
      }
    }
  )
}

case class SchemaList (
    schema: Seq[Schema]
) extends Response

object SchemaList {
  implicit val format: Format[SchemaList] = Json.format
}

class ResultTooBig extends Exception("The datsaet is too big to copy.  Try a sample or a LIMIT query instead.")

object Query
  extends LazyLogging
  with TimerUtils
{
  val RESULT_THRESHOLD = 10000

  def apply(
    query: String,
    includeCaveats: Boolean,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): DataContainer = 
  {
    MimirAPI.catalog.populateSpark
    apply(sparkSession.sql(query), includeCaveats)
  }

  def apply(
    query: DataFrame,
    includeCaveats: Boolean
  ): DataContainer =
  {
    var df = query

    /////// We need the schema before any annotations to produce the right outputs
    val schema = getSchema(df)

    logger.trace(s"----------- RAW-QUERY-----------\nSCHEMA:{ ${schema.mkString(", ")} }\n${df.queryExecution.explainString(SelectedExplainMode)}")

    /////// Add a __MIMIR_ROWID attribute
    df = AnnotateWithRowIds(df)

    logger.trace(s"----------- AFTER-ROWID -----------\n${df.queryExecution.explainString(SelectedExplainMode)}")

    /////// If requested, add a __CAVEATS attribute
    /////// Either way, after we track the caveats, we no longer need the
    /////// ApplyCaveat decorators
    if(includeCaveats){ df = df.trackCaveats.stripCaveats }
    else              { df = df.stripCaveats }
    
    logger.trace("############ FOOO")
    logger.trace(s"############ \n${df.queryExecution.analyzed.treeString}")

    logger.trace(s"----------- AFTER-CAVEATS -----------\n${df.queryExecution.explainString(SelectedExplainMode)}")


    /////// Create a mapping from field name to position in the output tuples
    val postAnnotationSchema = 
      getSchema(df)
        .zipWithIndex
        .map { case (attribute, idx) => attribute.name -> idx }
        .toMap

    /////// Compute attribute positions for later extraction
    val fieldIndices = 
      schema.map { attribute => postAnnotationSchema(attribute.name) }
    val identifierAnnotation = postAnnotationSchema(AnnotateWithRowIds.ATTRIBUTE)

    /////// Actually compute the final result
    val results = logTime("QUERY", df.toString) {
      val buffer = df.cache().take(RESULT_THRESHOLD+1)
      if(buffer.size >= RESULT_THRESHOLD){ 
        throw new ResultTooBig()
      }
      buffer
    }

    /////// If necessary, extract which rows/cells are affected by caveats from
    /////// the result table.
    val (colTaint, rowTaint): (Seq[Seq[Boolean]], Seq[Boolean]) = 
      if(includeCaveats){
        results.map { row =>
          val annotation = row.getAs[Row](Caveats.ANNOTATION_ATTRIBUTE)
          val columnAnnotations = annotation.getAs[Row](Caveats.ATTRIBUTE_FIELD)
          (
            schema.map { attribute => columnAnnotations.getAs[Boolean](attribute.name) },
            annotation.getAs[Boolean](Caveats.ROW_FIELD)
          )
        }.toSeq.unzip[Seq[Boolean], Boolean]
      } else { (Seq[Seq[Boolean]](), Seq[Boolean]()) }

    /////// Dump the final results.
    DataContainer(
      schema,
      results.map { row => fieldIndices.map { row.get(_) } }.toSeq,
      // use s"" instead of .toString below to handle nulls correctly
      results.map { row => s"${row.get(identifierAnnotation)}" }.toSeq,
      colTaint, 
      rowTaint,
      Seq()
    )
  }

  def getSchema(df: DataFrame):Seq[Schema] = 
    df.schema match { 
      case StructType(fields) => 
        fields.map { field => Schema(field.name, field.dataType) }
      case other => 
        throw new IllegalArgumentException(s"Query produces a non-dataframe output $other")
    }

  def getSchema(
    query: String,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): Seq[Schema] = { 
    MimirAPI.catalog.populateSpark
    getSchema(sparkSession.sql(query))
  }

}