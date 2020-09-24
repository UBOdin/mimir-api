package org.mimirdb.api.request

import org.apache.spark.sql.{ SparkSession, DataFrame, Row }
import org.apache.spark.sql.types.{ StructType, StructField }
import org.apache.spark.sql.execution.{ ExtendedMode => SelectedExplainMode }
import org.apache.spark.sql.SparkSession

import play.api.libs.json._

import org.mimirdb.api.{ Request, Response, JsonResponse, MimirAPI }
import org.mimirdb.caveats.{ Caveat, CaveatSet, Constants => Caveats }
import org.mimirdb.caveats.implicits._
import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimirdb.spark.{ SparkPrimitive, Schema }
import org.mimirdb.spark.Schema.fieldFormat
import org.mimirdb.api.CaveatFormat._
import org.mimirdb.util.TimerUtils

import com.typesafe.scalalogging.LazyLogging
import org.mimirdb.lenses.AnnotateImplicitHeuristics
import org.mimirdb.rowids.AnnotateWithSequenceNumber
import org.mimirdb.spark.InjectedSparkSQL

case class QueryMimirRequest (
            /* input for query */
                  input: Option[String],
            /* tables for query */
                  views: Option[Map[String,String]],
            /* query string - sql */
                  query: String,
            /* include taint in response */
                  includeUncertainty: Option[Boolean],
            /* include reasons in response */
                  includeReasons: Option[Boolean]
) extends Request {
  def handle = {
    if(!input.getOrElse("").equals("")){
      throw new UnsupportedOperationException("Input substitutions are no longer supported")
    }
    if(includeReasons.getOrElse(false)) {
      throw new UnsupportedOperationException("IncludeReasons is no longer supported")
    }
    Query(
      query,
      includeUncertainty.getOrElse(true),
      views = views.map { _.mapValues { baseTable => 
                        { () => MimirAPI.catalog.get(baseTable) }
                    } } 
                   .getOrElse { MimirAPI.catalog.allTableConstructors }
    )
  }
}

object QueryMimirRequest {
  implicit val format: Format[QueryMimirRequest] = Json.format
}

/***************************************************************************************/

case class QueryTableRequest (
            /* input for query */
                  table: String,
            /* columns for query (`*` if omitted) */
                  columns: Option[Seq[String]],
            /* max number of rows to return */
                  limit: Option[Int],
            /* starting point to begin returning rows */
                  offset: Option[Long],
            /* include taint in response */
                  includeUncertainty: Boolean
) extends Request {
  def handle = {
    var df = MimirAPI.catalog.get(table)
    
    val columnNames:Seq[String] = 
      columns.getOrElse { df.schema.fieldNames }

    if(!offset.isEmpty){
      df = AnnotateWithSequenceNumber(df)
      df = df.filter(df(AnnotateWithSequenceNumber.ATTRIBUTE) >= offset.get)
    }

    // Filter down to the right columns... dropping the sequence number if needed
    df = df.select(columnNames.map { df(_) }:_*)

    Query(
      df,
      includeCaveats = includeUncertainty,
      limit = limit,
      computedProperties = MimirAPI.catalog.getProperties(table)
    )
  }
}

object QueryTableRequest {
  implicit val format: Format[QueryTableRequest] = Json.format
}

/***************************************************************************************/

case class SchemaForQueryRequest (
            /* query string to get schema for - sql */
                  query: String
) extends Request {
  def handle = SchemaList(Query.getSchema(query), Map.empty)
}

object SchemaForQueryRequest {
  implicit val format: Format[SchemaForQueryRequest] = Json.format
}

/***************************************************************************************/

case class SchemaForTableRequest (
            /* table name */
                  table: String
) extends Request {
  def handle = SchemaList(
    Schema(MimirAPI.catalog.get(table)),
    MimirAPI.catalog.getProperties(table)
  )
}

object SchemaForTableRequest {
  implicit val format: Format[SchemaForTableRequest] = Json.format
}

/***************************************************************************************/

case class SizeOfTableRequest (
            /* table name */
                  table: String
) extends Request {
  def handle = TableSize(
    MimirAPI.catalog.get(table).count()
  )
}

object SizeOfTableRequest {
  implicit val format: Format[SizeOfTableRequest] = Json.format
}

/***************************************************************************************/

case class DataContainer (
                  schema: Seq[StructField],
                  data: Seq[Seq[Any]],
                  prov: Seq[String],
                  colTaint: Seq[Seq[Boolean]],
                  rowTaint: Seq[Boolean],
                  reasons: Seq[Seq[Caveat]],
                  properties: Map[String,JsValue]
) extends JsonResponse[DataContainer]

object DataContainer {
  implicit val format: Format[DataContainer] = Format(
    new Reads[DataContainer] {
      def reads(data: JsValue): JsResult[DataContainer] =
      {
        val parsed = data.as[Map[String,JsValue]]
        val schema = parsed("schema").as[Seq[StructField]]
        val sparkSchema = schema.map { _.dataType }
        JsSuccess(
          DataContainer(
            schema,
            parsed("data").as[Seq[Seq[JsValue]]].map { 
              _.zip(sparkSchema).map { case (dat, sch) => 
                SparkPrimitive.decode(dat, sch) 
              } 
            },
            parsed("prov").as[Seq[String]],
            parsed("colTaint").as[Seq[Seq[Boolean]]],
            parsed("rowTaint").as[Seq[Boolean]],
            parsed("reasons").as[Seq[Seq[Caveat]]],
            parsed("properties").as[Map[String,JsValue]]
          )
        )
      }
    },
    new Writes[DataContainer] { 
      def writes(data: DataContainer): JsValue = {
        val sparkSchema = data.schema.map { _.dataType }
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
          "reasons" -> data.reasons,
          "properties" -> data.properties
        )
      }
    }
  )
}

/***************************************************************************************/

case class SchemaList (
    schema: Seq[StructField],
    properties: Map[String, JsValue]
) extends JsonResponse[SchemaList]

object SchemaList {
  implicit val format: Format[SchemaList] = Json.format
}

/***************************************************************************************/

case class TableSize (
    size: Long,
) extends JsonResponse[TableSize]

object TableSize {
  implicit val format: Format[TableSize] = Json.format
}

/***************************************************************************************/

class ResultTooBig extends Exception("The datsaet is too big to copy.  Try a sample or a LIMIT query instead.")

/***************************************************************************************/

object Query
  extends LazyLogging
  with TimerUtils
{
  val RESULT_THRESHOLD = 10000

  def apply(
    query: String,
    includeCaveats: Boolean,
    limit: Option[Int] = None,
    sparkSession: SparkSession = MimirAPI.sparkSession,
    views: Map[String, () => DataFrame] = MimirAPI.catalog.allTableConstructors
  ): DataContainer = 
  {
    apply(
      InjectedSparkSQL(sparkSession)(query, views),
      includeCaveats = includeCaveats, 
      limit = limit,
      computedProperties = Map.empty
    )
  }

  def apply(
    query: DataFrame,
    includeCaveats: Boolean
  ): DataContainer =
  {
    apply(
      query, 
      includeCaveats = includeCaveats, 
      limit = None,
      computedProperties = Map.empty
    )
  }
  def apply(
    query: DataFrame,
    includeCaveats: Boolean,
    limit: Option[Int],
    computedProperties: Map[String,JsValue]
  ): DataContainer =
  {

    // The order of operations in this method is very methodically selected:
    // - AnnotateWithRowIds MUST come before any operation that modifies UNION operators, since
    //   the order of the children affects the identity of their elements.
    // - AnnotateImplicitHeuristics MUST come before any operation that removes View markers (this
    //   includes AnnotateWithRowIds and caveat.trackCaveats)

    var df = query

    /////// Decorate any potentially erroneous heuristics
    df = AnnotateImplicitHeuristics(df)

    /////// We need the schema before any annotations to produce the right outputs
    val schema = Schema(df)

    logger.trace(s"----------- RAW-QUERY-----------\nSCHEMA:{ ${schema.mkString(", ")} }\n${df.queryExecution.explainString(SelectedExplainMode)}")

    /////// Add a __MIMIR_ROWID attribute
    df = AnnotateWithRowIds(df)

    logger.trace(s"----------- AFTER-ROWID -----------\n${df.queryExecution.explainString(SelectedExplainMode)}")

    /////// If requested, add a __CAVEATS attribute
    /////// Either way, after we track the caveats, we no longer need the
    /////// ApplyCaveat decorators
    if(includeCaveats){ df = df.trackCaveats.stripCaveats }
    else              { df = df.stripCaveats }
    
    logger.trace(s"############ \n${df.queryExecution.analyzed.treeString}")
    logger.trace("############")

    logger.trace(s"----------- AFTER-CAVEATS -----------\n${df.queryExecution.explainString(SelectedExplainMode)}")

    if(!limit.isEmpty){
      df = df.limit(limit.get)
      logger.trace(s"----------- AFTER-LIMIT -----------\n${df.queryExecution.explainString(SelectedExplainMode)}")
    }

    /////// Create a mapping from field name to position in the output tuples
    val postAnnotationSchema = 
      Schema(df)
        .zipWithIndex
        .map { case (attribute, idx) => attribute.name.toLowerCase -> idx }
        .toMap

    /////// Compute attribute positions for later extraction
    val fieldIndices = 
      schema.map { attribute => postAnnotationSchema(attribute.name.toLowerCase) }
    val identifierAnnotation = postAnnotationSchema(AnnotateWithRowIds.ATTRIBUTE.toLowerCase)

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
      Seq(),
      computedProperties
    )
  }

  def getSchema(
    query: String,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): Seq[StructField] = { 
    val df = InjectedSparkSQL(sparkSession)(query, MimirAPI.catalog.allTableConstructors)
    Schema(df)
  }

}