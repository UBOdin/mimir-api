package org.mimirdb.api

import play.api.libs.json._
import org.mimirdb.api.tasks._
import org.mimirdb.api.tasks.CreateSample.modeFormat

sealed abstract class Request {
  def handle : JsValue
}

object Request {
  
}

case class CodeEvalRequest (
            /* scala source code to evaluate*/
                  input: Map[String,String],
                  language: String,
                  source: String
) extends Request {
  def handle = {
    Json.toJson(
      language match {
        case "R"     => EvalR(source)
        case "scala" => EvalScala(source)
      }
    )
  }
}

object CodeEvalRequest {
  implicit val format: Format[CodeEvalRequest] = Json.format
}

case class LoadRequest (
            /* file url of datasorce to load */
                  file: String,
            /* format of file for spark */
                  format: String,
            /* infer types in data source */
                  inferTypes: Boolean,
            /* detect headers in datasource */
                  detectHeaders: Boolean,
            /* optionally provide a name */
                  humanReadableName: Option[String],
            /* options for spark datasource api */
                  backendOption: Seq[Tuple],
            /* optionally provide dependencies */
                  dependencies: Seq[String],
            /* optionally provide an output name */
                  resultName: Option[String]
) extends Request {
  def handle = 
    Json.toJson(LoadResponse(LoadDataset(
      file, 
      format, 
      inferTypes, 
      detectHeaders, 
      humanReadableName, 
      backendOption.map { tup => tup.name -> tup.value }, 
      dependencies,
      resultName
    )))
}

object LoadRequest {
  implicit val format: Format[LoadRequest] = Json.format
}

case class UnloadRequest (
            /* table or view to unload */
                  input: String,
            /* file url of datasorce to unload */
                  file: String,
            /* format of file for spark */
                  format: String,
            /* options for spark datasource api */
                  backendOption: Seq[Tuple]
) extends Request {
  def handle = 
    Json.toJson(UnloadResponse(UnloadDataset(
      input,
      file,
      format,
      backendOption.map { tup => tup.name -> tup.value } 
    )))
}

object UnloadRequest {
  implicit val format: Format[UnloadRequest] = Json.format
}


case class CreateLensRequest (
            /* input for lens */
                  input: String,
            /* configuration parameters */
                  params: JsValue,
            /* type name of lens */
                  `type`: String,
            /* materialize input before creating lens */
                  materialize: Boolean,
            /* optional human-readable name */
                  humanReadableName: Option[String],
            /* optional name for the result table */
                  resultName: Option[String]
) extends Request {
  def handle = 
    Json.toJson(CreateLensResponse(CreateLens(
      input,
      params,
      `type`,
      materialize,
      humanReadableName,
      resultName
    )))
}

object CreateLensRequest {
  implicit val format: Format[CreateLensRequest] = Json.format
}


case class CreateViewRequest (
            /* temporary view definitions for use in creating the view */
                  input: Map[String,String],
            /* query for view */
                  query: String,
            /* optional name for the result table */
                  resultName: Option[String]
)  extends Request {
  def handle = 
    Json.toJson(CreateViewResponse(CreateView(
      input, 
      query,
      resultName
    )))
}

object CreateViewRequest {
  implicit val format: Format[CreateViewRequest] = Json.format
}

case class ExplainSubsetWithoutSchemaRequest (
            /* query to explain */
                  query: String,
                  rows: Seq[String],
                  cols: Seq[String]
) extends Request {
  def handle = Json.toJson(ExplainResponse(Explain.coarsely(
    query,
    rows = rows,
    cols = cols,
    schemaCaveats = false
  )))
}

object ExplainSubsetWithoutSchemaRequest {
  implicit val format: Format[ExplainSubsetWithoutSchemaRequest] = Json.format
}


case class ExplainSchemaRequest (
            /* query to explain */
                  query: String,
                  cols: Seq[String]
) extends Request {
  def handle = Json.toJson(ExplainResponse(Explain.coarsely(
    query,
    cols = cols
  )))
}

object ExplainSchemaRequest {
  implicit val format: Format[ExplainSchemaRequest] = Json.format
}


case class ExplainCellSchemaRequest (
            /* query to explain */
                  query: String,
            /* rowid of cell */
                  row: String,
            /* column of cell */
                  col: String
) extends Request {
  def handle = Json.toJson(ExplainReasonsResponse(Explain(
    query,
    rows = Seq(row),
    cols = Seq(col)
  )))
}

object ExplainCellSchemaRequest {
  implicit val format: Format[ExplainCellSchemaRequest] = Json.format
}


case class ExplainSubsetRequest (
            /* query to explain */
                  query: String,
                  rows: Seq[String],
                  cols: Seq[String]
) extends Request {
  def handle = Json.toJson(ExplainResponse(Explain.coarsely(
    query,
    rows = rows,
    cols = cols
  )))
}

object ExplainSubsetRequest {
  implicit val format: Format[ExplainSubsetRequest] = Json.format
}


case class ExplainEverythingAllRequest (
            /* query to explain */
                  query: String
) extends Request {
  def handle = Json.toJson(ExplainReasonsResponse(Explain(
    query
  )))
}

object ExplainEverythingAllRequest {
  implicit val format: Format[ExplainEverythingAllRequest] = Json.format
}


case class ExplainEverythingRequest (
            /* query to explain */
                  query: String
) extends Request {
  def handle = Json.toJson(ExplainResponse(Explain.coarsely(
    query
  )))
}

object ExplainEverythingRequest {
  implicit val format: Format[ExplainEverythingRequest] = Json.format
}


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
    Json.toJson(Query(
      inputSubstitutionQuery,
      includeUncertainty,
      includeReasons
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


case class CreateSampleRequest (
            /* query string to get schema for - table name */
                  source: String,
            /* mode configuration */
                  samplingMode: CreateSample.SamplingMode,
            /* seed - optional long */
                  seed: Option[Long],
            /* optional name for the result table */
                  resultName: Option[String]
) extends Request {
  def handle = {
    val target = 
      resultName.getOrElse {
        s"SAMPLE_${(source+samplingMode.toString+seed.toString).hashCode().toString().replace("-", "")}"
      }
    CreateSample(
      source, 
      target,
      samplingMode,
      seed
    )
    Json.toJson(CreateSampleResponse(target))
  }
}

object CreateSampleRequest {
  implicit val format: Format[CreateSampleRequest] = Json.format
}

