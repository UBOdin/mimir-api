package org.mimirdb.data

import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.types._
import play.api.libs.json._
import com.typesafe.scalalogging.LazyLogging
import java.sql.SQLException

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

  def this(sqlitedb: String, spark: SparkSession, downloads:String) = 
    this(
      new JDBCMetadataBackend("sqlite", sqlitedb),
      new LocalFSStagingProvider(downloads),
      spark
    )

  def this(sqlitedb: String, spark: SparkSession) = 
    this(sqlitedb, spark, "vizier_downloads")

  val views = metadata.registerMap("MIMIR_VIEWS", Seq(
    InitMap(Seq(
      "TYPE"         -> StringType,
      "CONSTRUCTOR"  -> StringType,
      "DEPENDENCIES" -> StringType
    ))
  ))

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
      var parser = spark.read.format(format)
      for((option, value) <- sparkOptions){
        parser = parser.option(option, value)
      }
      var tempDf = parser.load(url)
      (
        staging.stage(
          parser.load(url),
          bulkStorageFormat,
          Some(tableName)
        ),
        Map(),
        bulkStorageFormat
      )
    }
  }

  def put[T <: DataFrameConstructor](
    name: String, 
    constructor: T, 
    dependencies: Set[String], 
    replaceIfExists: Boolean = true
  )(implicit format: Format[T]): DataFrame = {
    if(!replaceIfExists && views.exists(name)){
      throw new SQLException(s"View $name already exists")
    }

    val context = 
      dependencies
        .toSeq
        .map { dep => dep -> get(dep) }
        .toMap 

    val df = constructor.construct(spark, context)

    cache.put(name, df)

    views.put(
      name,
      Seq(
        constructor.deserializer.toString,
        Json.toJson(constructor).toString,
        Json.toJson(dependencies.toSeq).toString
      )
    )

    return df
  }


  def get(name: String): DataFrame = 
  {
    if(cache contains name){
      return cache(name)
    }

    val (_, components) = views.get(name).get
    val deserializerClassName = 
      components(0).asInstanceOf[String]
    val constructorJson = 
      Json.parse(components(1).asInstanceOf[String])
    val dependencies = 
      Json.parse(components(2).asInstanceOf[String])
        .as[Seq[String]]
        .map { dep => dep -> get(dep) }
        .toMap

    val deserializerClass = 
      Class.forName(deserializerClassName)
    val deserializer: DataFrameConstructorCodec = 
      deserializerClass
           .getField("MODULE$")
           .get(deserializerClass)
           .asInstanceOf[DataFrameConstructorCodec]
    val constructor = deserializer(constructorJson)

    val df = constructor.construct(spark, dependencies)

    cache.put(name, df)

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
    "mode" -> "DROPMALFORMED"
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
      case FileFormat.CSV | FileFormat.CSV_WITH_ERRORCHECKING =>
        defaultLoadCSVOptions ++ Map("header" -> header.toString)
      case FileFormat.GOOGLE_SHEETS => defaultLoadGoogleSheetOptions
    }
  }

  val usesDatasourceErrors = Set(
    FileFormat.CSV_WITH_ERRORCHECKING
  )

}