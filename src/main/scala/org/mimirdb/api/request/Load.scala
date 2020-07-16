package org.mimirdb.api.request

import java.io.{
  IOException,
  FileNotFoundException
}
import play.api.libs.json._
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.types.StructField
import com.typesafe.scalalogging.LazyLogging

import org.mimirdb.api.{ 
  Request, 
  Response, 
  Tuple, 
  MimirAPI,
  FormattedError,
  CreateResponse
}
import org.mimirdb.data.{ 
  Catalog, 
  FileFormat, 
  LoadConstructor,
  InlineConstructor
}
import org.mimirdb.lenses.Lenses
import org.mimirdb.spark.Schema
import org.mimirdb.spark.Schema.fieldFormat


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
                  dependencies: Option[Seq[String]],
            /* optionally provide an output name */
                  resultName: Option[String],
            /* optional properties */
                  properties: Option[Map[String,JsValue]]
) extends Request {

  lazy val output = 
    resultName.getOrElse {
      val lensNameBase = (
        file 
        + format 
        + inferTypes.toString 
        + detectHeaders.toString 
        + humanReadableName.toString 
        + backendOption.toString 
        + dependencies.toString
      ).hashCode()
      val hint = humanReadableName.getOrElse { format }.replaceAll("[^a-zA-Z]", "")
      "DATASOURCE_" + hint + "_" + (lensNameBase.toString().replace("-", ""))
    }

  def handle: CreateResponse = {
    try { 
      val sparkOptions = backendOption.map { tup => tup.name -> tup.value }

      // Some parameters may change during the loading process.  Var-ify them
      var url = file
      var storageFormat = format
      var finalSparkOptions = 
        Catalog.defaultLoadOptions(format, detectHeaders) ++ sparkOptions

      // Build a preliminary configuration of Mimir-specific metadata
      val mimirOptions = scala.collection.mutable.Map[String, JsValue]()

      val stagingIsMandatory = (
           file.startsWith("http://")
        || file.startsWith("https://")
      )
      // Do some pre-processing / default configuration for specific formats
      //  to make the API a little friendlier.
      storageFormat match {

        // The Google Sheets loader expects to see only the last two path components of 
        // the sheet URL.  Rewrite full URLs if the user wants.
        case FileFormat.GOOGLE_SHEETS => {
          url = url.split("/").reverse.take(2).reverse.mkString("/")
        }
        
        // For everything else do nothing
        case _ => {}
      }

      if(stagingIsMandatory) {
        // Preserve the original URL and configurations in the mimirOptions
        mimirOptions("preStagedUrl") = JsString(url)
        mimirOptions("preStagedSparkOptions") = Json.toJson(finalSparkOptions)
        mimirOptions("preStagedFormat") = JsString(storageFormat)
        val stagedConfig  = MimirAPI.catalog.stage(url, finalSparkOptions, storageFormat, output)
        url               = stagedConfig._1
        finalSparkOptions = stagedConfig._2
        storageFormat     = stagedConfig._3
      }

      var loadConstructor = LoadConstructor(
        url = url,
        format = storageFormat,
        sparkOptions = finalSparkOptions,
        contextText = humanReadableName
      )

      // Infer types if necessary
      if(inferTypes){
        loadConstructor = loadConstructor.withLens(
          MimirAPI.sparkSession, 
          "TYPE_INFERENCE", 
          "in " +humanReadableName.getOrElse { file }
        )
      }

      // And finally save the dataframe constructor
      val df = MimirAPI.catalog.put(
        name = output, 
        constructor = loadConstructor, 
        dependencies = dependencies.getOrElse { Seq() }.toSet, 
        properties = properties.getOrElse { Map.empty }
      )
      return CreateResponse(output, Schema(df), properties.getOrElse { Map.empty })
    } catch {
      case e: FileNotFoundException => 
        throw FormattedError(e, s"Can't Load URL [Not Found]: $file")
      case e: IOException => 
        throw FormattedError(e, s"Error Loading $file (${e.getMessage()}")
    }

  }
}

object LoadRequest {
  implicit val format: Format[LoadRequest] = Json.format
}

case class LoadInlineRequest(
  schema: Seq[StructField],
  data: Seq[Seq[JsValue]],
  dependencies: Option[Seq[String]],
  resultName: Option[String],
  properties: Option[Map[String, JsValue]],
  humanReadableName: Option[String],
) extends Request
{
  lazy val output = 
    resultName.getOrElse {
      val lensNameBase = (
        schema 
        + data.hashCode().toString
        + dependencies.hashCode().toString
        + properties.hashCode().toString
      ).hashCode()
      val hint = humanReadableName.getOrElse { "UNNAMED_TABLE" }.replaceAll("[^a-zA-Z]", "")
      "INLINE_" + hint + "_" + (lensNameBase.toString().replace("-", ""))
    }

  def handle: CreateResponse =
  {
    val df = MimirAPI.catalog.put(
      name = output,
      constructor = InlineConstructor(schema, data),
      dependencies = dependencies.getOrElse { Seq() }.toSet, 
      properties = properties.getOrElse { Map.empty }
    )
    CreateResponse(output, Schema(df), properties.getOrElse { Map.empty })
  }
}

object LoadInlineRequest {
  implicit val format: Format[LoadInlineRequest] = Json.format
}
