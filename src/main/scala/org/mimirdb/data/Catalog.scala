package org.mimirdb.data

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.types._
import play.api.libs.json._
import com.typesafe.scalalogging.LazyLogging
import java.sql.SQLException
import java.net.URI
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.mimirdb.spark.PythonUDFBuilder
import org.mimirdb.profiler.DataProfiler
import org.mimirdb.util.ExperimentalOptions
import org.mimirdb.util.TaskDeduplicator
import org.mimirdb.rowids.AnnotateWithRowIds

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
  val staging: StagingProvider,
  spark: SparkSession,
  bulkStorageFormat: FileFormat.T = FileFormat.PARQUET,
) extends LazyLogging
{
  val cache = scala.collection.concurrent.TrieMap[String, DataFrame]()
  val profilingDeduplicator = new TaskDeduplicator[Map[String, JsValue]]()
 
  def this(sqlitedb: String, spark: SparkSession, downloads:String, downloadsIsRelativeToDataDir: Boolean) = 
    this(
      new JDBCMetadataBackend("sqlite", sqlitedb),
      new LocalFSStagingProvider(downloads, downloadsIsRelativeToDataDir),
      spark
    )

  def this(sqlitedb: String, spark: SparkSession) = 
    this(sqlitedb, spark, "vizier_downloads", true)

  val views = metadata.registerMap("MIMIR_VIEWS", Seq(
    InitMap(Seq(
      "TYPE"         -> StringType,
      "CONSTRUCTOR"  -> StringType,
      "DEPENDENCIES" -> StringType
    )),
    AddColumnToMap(
      "PROPERTIES",
      StringType,
      Some("{}")
    )
  ))

  /**
   * Stage the provided URL into the local filesystem if necessary
   * 
   * @param url           The URL to stage into the local filesystem
   * @param sparkOptions  Options to provide to the LoadConstructor
   * @param format        The format to stage the provided file as (if needed)
   * @param tableName     The name of the table to associate with the file
   * @return              A four tuple of: (i) The staged URL, (ii) the sparkOptions
   *                      parameter passed, (iii) The format used to stage the file
   *                      and (iv) Whether the returned url is relative to the data
   *                      directory.
   *
   * This function takes a URL and rewrites it to allow efficient local access using
   * the Catalog's defined staging provider.  Generally, this means copying the URL
   * into the local filesystem.
   * 
   * The behavior of this function is governed by the type of URL and the provided
   * format.  Some URL protocols are exempt from staging, e.g., s3a links, since
   * spark interacts with these directly (and presumably efficiently).  Apart from
   * that the staging can be handled either "raw" or via spark depending on the
   * format.  For example, a CSV file can be downloaded and its raw bytestream
   * can just be saved on disk.  Other formats like google sheets require a level
   * of indirection, using the corresponding spark dataloader to bring the file into
   * spark and then dumping it out in e.g., parquet format.
   */
  def stage(
    url: String, 
    sparkOptions: Map[String,String], 
    format: String, 
    tableName: String
  ): (String, Map[String,String], String, Boolean) =
  {
    if(Catalog.stagingExemptProtocols(URI.create(url).getScheme)){
      ( 
        url,
        sparkOptions,
        format,
        false
      )
    } 
    else if(Catalog.safeForRawStaging(format)){
      val (stagedUrl, relative) = staging.stage(url, Some(tableName))
      ( 
        stagedUrl,
        sparkOptions,
        format,
        relative
      )
    } else {
      var parser = spark.read.format(format)
      for((option, value) <- sparkOptions){
        parser = parser.option(option, value)
      }
      var tempDf = parser.load(url)
      val (stagedUrl, relative) = 
        staging.stage(
          parser.load(url),
          bulkStorageFormat,
          Some(tableName)
        )
      (
        stagedUrl,
        Map(),
        bulkStorageFormat,
        relative
      )
    }
  }

  /**
   * Stage the provided dataframe and assign the staged version a name
   *
   * @param name            The name of the dataframe to create
   * @param df              The dataframe to stage
   * @param format          The storage format to use for the dataframe
   * @param replaceIfExists Don't fail if a dataframe with the same name exists
   * @param properties      The mimir metadata to attach to the created dataframe
   * @param proposedSchema  (optional) The schema of the provided dataframe
   * @param includeRowId    Include the dataframe's existing rowid column
   * @param dependencies    Any tables that were used to create this dataframe
   */
  def stageAndPut(
    name: String,
    df: DataFrame,
    format: String = bulkStorageFormat,
    replaceIfExists: Boolean = true,
    properties: Map[String, JsValue] = Map.empty,
    proposedSchema: Option[Seq[StructField]] = None,
    includeRowId: Boolean = true,
    dependencies: Set[String] = Set.empty
  )
  {
    val (url, relative) = staging.stage(
      input = (
        if(includeRowId) { AnnotateWithRowIds(df) }
        else { df }
      ), 
      format = format, 
      nameHint = Some(name)
    )
    put(
      name = name, 
      constructor = LoadConstructor(
        url = url, 
        format = format, 
        sparkOptions = Map(), 
        lenses = Seq(), 
        contextText = None, 
        proposedSchema = proposedSchema.map { _ ++ (
          if(includeRowId) { 
            Some(StructField(AnnotateWithRowIds.ATTRIBUTE, StringType))
          } else { None }
        )} ,
        urlIsRelativeToDataDir = Some(relative)
      ),
      dependencies = dependencies,
      replaceIfExists = replaceIfExists,
      properties = properties
    )
  }

  def put[T <: DataFrameConstructor](
    name: String, 
    constructor: T, 
    dependencies: Set[String], 
    replaceIfExists: Boolean = true,
    properties: Map[String, JsValue] = Map.empty,
    runProfiler: Boolean = ExperimentalOptions.isEnabled("PROFILE-EVERYTHING")
  )(implicit format: Format[T]): DataFrame = {
    if(!replaceIfExists && views.exists(name)){
      throw new SQLException(s"View $name already exists")
    }

    val context = 
      dependencies
        .toSeq
        .map { dep => dep -> { () => get(dep) } }
        .toMap 

    logger.debug(s"PUT $name:\n$constructor")
    val df = constructor.construct(spark, context)

    // Retain the data frame locally
    cache.put(name, df)

    logger.debug(s"... with dataframe:\n${df.queryExecution.analyzed.treeString}")

    val profile = if(runProfiler){ 
      logger.debug("... running profiler")
      DataProfiler(df) 
    } else { Map() }

    logger.debug(s"... with properties:\n${profile ++ properties}")

    // Save the view
    views.put(
      name,
      Seq(
        constructor.deserializer.toString,
        Json.toJson(constructor).toString,
        Json.toJson(dependencies.toSeq).toString,
        Json.toJson(profile ++ properties).toString
      )
    )

    return df
  }

  def getProperties(name: String): Map[String, JsValue] =
  {
    synchronized {
      val (_, components) = views.get(name).getOrElse {
        throw new UnresolvedException(
          UnresolvedRelation(Seq(name)),
          "lookup"
        )
      }
      Json.parse(components(3).asInstanceOf[String])
        .as[Map[String,JsValue]]
    }
  }

  def setProperties(name: String, properties: Map[String, JsValue])
  {
    synchronized {
      val (field, components) = views.get(name).getOrElse {
        throw new UnresolvedException(
          UnresolvedRelation(Seq(name)),
          "lookup"
        )
      }
      views.put(field, 
        components.patch(3, Seq(Json.toJson(properties).toString), 1)
      )
    }
  }

  def updateProperties(name: String, properties: Map[String, JsValue])
  {
    synchronized {
      val (field, components) = views.get(name).getOrElse {
        throw new UnresolvedException(
          UnresolvedRelation(Seq(name)),
          "lookup"
        )
      }
      val oldProperties = 
        Json.parse(components(3).asInstanceOf[String])
          .as[Map[String,JsValue]]
      views.put(field, 
        components.patch(3, Seq(Json.toJson(oldProperties ++ properties).toString), 1)
      )
    }
  }

  def profile(name: String): Map[String,JsValue] =
  {
    val properties = getProperties(name)
    if(properties contains DataProfiler.IS_PROFILED){
      return properties
    } else {
      return profilingDeduplicator.deduplicate(name) {
        val df = get(name)
        val properties = DataProfiler(df)
        updateProperties(name, properties)
        properties
      }
    }
  }

  def getConstructor(
    name: String
  ): (DataFrameConstructor, Seq[String]) =
  {
    val (_, components) = views.get(name).getOrElse {
      throw new UnresolvedException(
        UnresolvedRelation(Seq(name)),
        "lookup"
      )

    }
    val deserializerClassName = 
      components(0).asInstanceOf[String]
    val constructorJson = 
      Json.parse(components(1).asInstanceOf[String])


    val dependencies = 
      Json.parse(components(2).asInstanceOf[String])
        .as[Seq[String]]

    val deserializerClass = 
      Class.forName(deserializerClassName)
    val deserializer: DataFrameConstructorCodec = 
      deserializerClass
           .getField("MODULE$")
           .get(deserializerClass)
           .asInstanceOf[DataFrameConstructorCodec]
    val constructor = deserializer(constructorJson)

    return (constructor, dependencies)
  }

  def get(name: String): DataFrame = 
  {
    if(cache contains name){
      return cache(name)
    }
    val (constructor, dependencies) = getConstructor(name)
    val df = constructor.construct(
      spark,
      dependencies
        .map { dep => dep -> { () => get(dep) } }
        .toMap
    )

    cache.put(name, df)

    return df
  }

  def getProvenance(name: String): DataFrame =
  {
    val (constructor, dependencies) = getConstructor(name)
    logger.trace(s"Get Provenance for $name / $constructor <- $dependencies")
    val df = constructor.provenance(
      spark, 
      dependencies
        .map { dep => dep -> { () => getProvenance(dep) } }
        .toMap
    )
    return df
  }

  def getOption(name: String): Option[DataFrame] = 
    cache.get(name) match {
      case s:Some[_] => s
      case None => 
        if(views.exists(name)) { Some(get(name)) }
        else { None }
    }

  def exists(name: String): Boolean =
    getOption(name) != None

  def flush(name: String)
  {
    cache.remove(name)
  }

  /**
   * Return a map of constructors (suitable for use with InjectedSparkSQL) for all tables
   * known to the catalog
   */
  def allTableConstructors: Map[String, () => DataFrame] =
    views.keys.map { k => k -> { () => get(k) }}.toMap

  def populateSpark(targets: Iterable[String] = views.keys, forgetInvalidTables: Boolean = false)
  {
    logger.error("populateSpark is deprecated.  Use org.mimirdb.spark.InjectedSparkSQL with Catalog.allTableConstructors instead")
    for(view <- views.keys){
      try {
        get(view).createOrReplaceTempView(view)
      } catch {
        //TODO: This is a hack to cleanup the logs for missing data files
        //  without forgetting invalid tables.  This does not prevent performance hit 
        //  trying to load the missing tables.  
        case pathNf:org.apache.spark.sql.AnalysisException 
          if pathNf.message.startsWith("Path does not exist") => {
            logger.warn(s"Couldn't safely preload $view: ${pathNf.message}")
            if(forgetInvalidTables){
              logger.warn(s"Forgetting $view")
              logger.warn(views.get(view).get._2.asInstanceOf[String])
              views.rm(view)
            }
          }
        case e:Exception => {
          logger.error(s"Couldn't safely preload $view.\n$e")
          if(forgetInvalidTables){
            logger.warn(s"Forgetting $view")
            logger.warn(views.get(view).get._2.asInstanceOf[String])
            views.rm(view)
          }
        }
      }
    }
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
  
  val stagingExemptProtocols = Set(
    DataSourceProtocol.S3A    
  )

  private val defaultLoadCSVOptions = Map(
    "ignoreLeadingWhiteSpace"-> "true",
    "ignoreTrailingWhiteSpace"-> "true"
  )
  
  private val defaultLoadGoogleSheetOptions = Map(
      "serviceAccountId" -> "vizier@api-project-378720062738.iam.gserviceaccount.com",
      "credentialPath" -> "test/data/api-project-378720062738-5923e0b6125f")
  
  def defaultLoadOptions(
    format: FileFormat.T, 
    header: Boolean
  ): Map[String,String] = 
  {
    format match {
      case FileFormat.CSV | FileFormat.CSV_WITH_ERRORCHECKING | FileFormat.EXCEL =>
        defaultLoadCSVOptions ++ Map("header" -> header.toString)
      case FileFormat.GOOGLE_SHEETS => defaultLoadGoogleSheetOptions
      case _ => Map()
    }
  }

  val usesDatasourceErrors = Set(
    FileFormat.CSV_WITH_ERRORCHECKING
  )

}