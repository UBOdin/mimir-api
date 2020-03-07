package org.mimirdb.data

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.types._
import play.api.libs.json._
import com.typesafe.scalalogging.LazyLogging


/**
 * Lazy data ingest and view management for Mimir
 * 
 * Spark provides a Hive/Derby data storage framework that
 * you can use to ingest and locally store data.  However, 
 * using this feature has several major drawbacks:
 *   - It pollutes the local working directory unless you're
 *     connected to a HDFS cluster or have S3 set up.
 *     https://github.com/UBOdin/mimir/issues/321
 *   - It forces all data access to go through Spark, even
 *     if it might be more efficient to do so locally
 *     https://github.com/UBOdin/mimir/issues/322
 *   - It makes processing multi-table files / URLs much
 *     harder by forcing all access to go through the
 *     Dataset API.  
 *   - It makes provenance tracking harder, since we lose
 *     track of exactly where tables came from, as well as
 *     which tables are ingested/managed by Mimir.  We also
 *     lose the original data / URL.
 *   - It uses unparsed SQL to store views, making all of the
 *     stuff that we do with caveats difficult to persist.
 *    
 * This class allows Mimir to provide lower-overhead 
 * data source management than Derby. Schema information
 * is stored in Mimir's metadata store, and LoadedTables
 * dynamically creates DataFrames whenever the table is 
 * queried.
 */
class Catalog(
  metadata: MetadataBackend, 
  staging: StagingProvider,
  spark: SparkSession,
  bulkStorageFormat: FileFormat.T = FileFormat.PARQUET,
) extends LazyLogging
{
  val cache = scala.collection.mutable.Map[String, DataFrame]()

  def this(sqlitedb: String, spark: SparkSession) = 
    this(
      new JDBCMetadataBackend("sqlite", sqlitedb),
      new LocalFSStagingProvider("vizier_downloads"),
      spark
    )

  val views = metadata.registerMap("MIMIR_VIEWS", Seq(
    InitMap(Seq(
      "DEFINITION" -> StringType,
    ))
  ))

  val tables = metadata.registerMap("MIMIR_TABLES", Seq(
    InitMap(Seq(
      "SOURCE"        -> StringType,
      "FORMAT"        -> StringType,
      "SPARK_OPTIONS" -> MapType(StringType, StringType),

    ))
  ))

  /**
   * Connect the specified URL to Mimir as a table.
   *
   * Note: This function does NOT stage data into a storage backend.
   * 
   * @param url           The URL to connect
   * @param format        The data loader to use
   * @param tableName     The name to give the URL
   * @param sparkOptions   Optional: Any pass-through parameters to provide the spark dataloader
   * @param mimirOptions   Optional: Any data loading parameters for Mimir itself (currently unused)
   */
  def linkTable(
    sourceFile: String, 
    format: String, 
    targetTable: String, 
    sparkOptions: Map[String,String] = Map(), 
    stageSourceURL: Boolean = false,
    replaceExisting: Boolean = true
  ) {
    if(tables.exists(targetTable)){
      if(replaceExisting){
        tables.rm(targetTable)
      } else {
        throw new Exception(s"Can't LOAD ${targetTable} because it already exists.")
      }
    }
    // Some parameters may change during the loading process.  Var-ify them
    var url = sourceFile
    var storageFormat = format
    var finalSparkOptions = 
      Catalog.defaultLoadOptions
             .getOrElse(format, Map()) ++ sparkOptions

    // Build a preliminary configuration of Mimir-specific metadata
    val mimirOptions = scala.collection.mutable.Map[String, JsValue]()

    val stagingIsMandatory = (
         sourceFile.startsWith("http://")
      || sourceFile.startsWith("https://")
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

    if(stageSourceURL || stagingIsMandatory) {
      // Preserve the original URL and configurations in the mimirOptions
      mimirOptions("preStagedUrl") = JsString(url)
      mimirOptions("preStagedSparkOptions") = Json.toJson(finalSparkOptions)
      mimirOptions("preStagedFormat") = JsString(storageFormat)
      val stagedConfig  = stage(url, finalSparkOptions, storageFormat, targetTable)
      url               = stagedConfig._1
      finalSparkOptions = stagedConfig._2
      storageFormat     = stagedConfig._3
    }

    // Not sure when exactly cache invalidation is needed within Spark.  It was in the old 
    // data loader, but it would only get called when the URL needed to be staged.  Since 
    // we're not staging here, might be safe to skip?  TODO: Ask Mike.
    // 
    // MimirSpark.get.sparkSession.sharedState.cacheManager.recacheByPath(
    //   MimirSpark.get.sparkSession, 
    //   url
    // )

    // Try to create the dataframe to make sure everything (parameters, etc...) are ok
    val df = makeDataFrame(
      url = url,
      format = storageFormat,
      sparkOptions = finalSparkOptions
    )
    // Cache the result
    cache.put(targetTable, df)

    // Save the parameters
    tables.put(
      targetTable, Seq(
        url,
        storageFormat,
        Json.toJson(finalSparkOptions),
        new JsObject(mimirOptions)
      )
    )
  }

  def stage(
    url: String, 
    sparkOptions: Map[String,String], 
    format: String, 
    tableName: String
  ): (String, Map[String,String], String) =
  {
    if(Catalog.safeForRawStaging(format)){
      ( 
        staging.stage(url, Some(tableName)),
        sparkOptions,
        format
      )
    } else {
      (
        staging.stage(
          makeDataFrame(url, format, sparkOptions),
          bulkStorageFormat,
          Some(tableName)
        ),
        Map(),
        bulkStorageFormat
      )
    }
  }

  def makeDataFrame(
    url: String, 
    format: String, 
    sparkOptions: Map[String, String]
  ): DataFrame = 
  {
    var parser = spark.read.format(format)
    for((option, value) <- sparkOptions){
      parser = parser.option(option, value)
    }
    logger.trace(s"Creating dataframe for $format file from $url")
    // parser = parser.schema(RAToSpark.mimirSchemaToStructType(customSchema))
    return parser.load(url)
  }

  def loadDataframe(table: String): Option[DataFrame] =
  {
    logger.trace(s"Loading $table")
         // If we have a record of the table (Some(tableDefinition))
    tables.get(table)
          // Then load the dataframe normally
          .map { tableDefinition => 
            makeDataFrame(
              url = tableDefinition._2(0).asInstanceOf[String],
              format = tableDefinition._2(1).asInstanceOf[String],
              sparkOptions = tableDefinition._2(2).asInstanceOf[JsObject].as[Map[String, String]]
            )
          }
          // and persist it in cache
          .map { df => cache.put(table, df); df }
          // otherwise fall through return None
  }
}

object Catalog
{
  val safeForRawStaging = Set(
    FileFormat.CSV ,
    FileFormat.CSV_WITH_ERRORCHECKING,
    FileFormat.JSON,
    FileFormat.EXCEL,
    FileFormat.XML,
    FileFormat.TEXT
  )

  private val defaultLoadCSVOptions = Map(
    "ignoreLeadingWhiteSpace"-> "true",
    "ignoreTrailingWhiteSpace"-> "true", 
    "mode" -> "DROPMALFORMED", 
    "header" -> "false"
  )
  
  private val defaultLoadGoogleSheetOptions = Map(
      "serviceAccountId" -> "vizier@api-project-378720062738.iam.gserviceaccount.com",
      "credentialPath" -> "test/data/api-project-378720062738-5923e0b6125f")
  
  val defaultLoadOptions = Map[String, Map[String,String]](
    FileFormat.CSV                    -> defaultLoadCSVOptions,
    FileFormat.CSV_WITH_ERRORCHECKING -> defaultLoadCSVOptions,
    FileFormat.GOOGLE_SHEETS -> defaultLoadGoogleSheetOptions
  )

  val usesDatasourceErrors = Set(
    FileFormat.CSV_WITH_ERRORCHECKING
  )

}