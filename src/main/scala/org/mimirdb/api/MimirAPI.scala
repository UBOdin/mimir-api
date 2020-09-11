package org.mimirdb.api

import org.eclipse.jetty.util.ssl.SslContextFactory
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.SecureRequestCustomizer
import org.eclipse.jetty.server.SslConnectionFactory
import org.eclipse.jetty.server.HttpConfiguration
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.http.HttpVersion
import org.eclipse.jetty.server.HttpConnectionFactory
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.server.handler.ResourceHandler
import org.eclipse.jetty.server.handler.ContextHandler
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerCollection
import org.eclipse.jetty.server.Handler
import org.eclipse.jetty.webapp.WebAppContext
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}
import scala.collection.JavaConversions._

import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json._

import java.sql.SQLException
import java.io.FileNotFoundException
import java.io.EOFException
import org.mimirdb.caveats.annotate.AnnotationException
import org.mimirdb.data.{
  Catalog,
  JDBCMetadataBackend,
  StagingProvider,
  LocalFSStagingProvider
}
import org.mimirdb.api.request._
import org.mimirdb.lenses.Lenses
import org.mimirdb.lenses.implementation.{
  GoogleGeocoder,
  OSMGeocoder
}
import org.mimirdb.util.JsonUtils.stringifyJsonParseErrors

import org.apache.spark.sql.AnalysisException
import org.mimirdb.data.MetadataBackend
import org.mimirdb.blobs.BlobStore
import org.mimirdb.util.ExperimentalOptions

//import org.apache.spark.ui.FixWebUi


object MimirAPI extends LazyLogging {
  
  var isRunning = true
  val DEFAULT_API_PORT = 8089

  var sparkSession: SparkSession = null
  var metadata: MetadataBackend = null
  var catalog: Catalog = null
  var server: Server = null
  var conf: MimirConfig = null
  var blobs: BlobStore = null

  def main(args: Array[String])
  {
    println("Starting Mimir API Server ...")
    logger.debug("debug is enabled")
    conf = new MimirConfig(args);
    conf.verify

    // Prepare Experiments
    ExperimentalOptions.enable(conf.experimental())

    // Initialize Spark
    sparkSession = InitSpark.local

    //Initialize GeoSpark
    InitSpark.initPlugins(sparkSession)
    
    // Initialize the catalog
    initCatalog(conf.metadata())

    // Initialize Geocoders (if configuration options available)
    val geocoders = 
      Seq(
        conf.googleAPIKey.map { new GoogleGeocoder(_) }.toOption,
        conf.osmServer.map { new OSMGeocoder(_) }.toOption
      ).flatten
    if(!geocoders.isEmpty){ Lenses.initGeocoding(geocoders, catalog) }

    // populate spark after lens initialization
    // For safety, drop any tables that are now invalid... it's easy to reload them later
    // catalog.populateSpark(forgetInvalidTables = true)
    
    // Start the server
    runServer(conf.port())

    //sparkSession.conf.set( "spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.CollapseProject")
    //println(Query.apply("SELECT TKEY,A,B FROM LENS_MISSING_VALUE_521717238", true, sparkSession))
    
    // And sleep until done
    println(s"... Mimir API Server Started on http://localhost:${conf.port()}/")
     while(isRunning){
       Thread.sleep(90000)
       
     }
     Thread.sleep(1000)
     server.stop();
  }
  
  def initCatalog(metadataDB: String)
  {
    metadata = metadataDB.split(":").toList match {
      case "sqlite" :: Nil => 
        new JDBCMetadataBackend("sqlite", s"${conf.dataDir()}vizier.db")
      case "sqlite" :: rest => 
        new JDBCMetadataBackend("sqlite", rest.mkString(":"))
      case _ => throw new IllegalArgumentException(s"Unknown metadata provider: ${metadataDB}")
    }
    val staging = new LocalFSStagingProvider(conf.staging())
    catalog = new Catalog(metadata, staging, sparkSession)
    blobs = new BlobStore(metadata)
  }
  
  def runServer(port: Int = DEFAULT_API_PORT) : Unit = {
    if(server != null){ 
      throw new RuntimeException("Can't have two Mimir servers running in one JVM")
    }
    //FixWebUi.fixSparkUi(sparkSession)
    server = new Server(port)
    val http_config = new HttpConfiguration();
    server.addConnector(new ServerConnector( server,  new HttpConnectionFactory(http_config)) );
    
    val contextHandler = buildSwaggerUI()
    
    val resource_handler2 = new ResourceHandler()
    resource_handler2.setDirectoriesListed(true)
    //println(s"${new java.io.File("./client/target/scala-2.12/scalajs-bundler").getAbsolutePath()}")
    resource_handler2.setResourceBase("./src")
    val contextHandler2 = new ContextHandler("/src");
    contextHandler2.setResourceBase("./src");
    contextHandler2.setHandler(resource_handler2);
     
    val servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
    servletContextHandler.setContextPath("/");
    val holder = new ServletHolder(new MimirVizierServlet());
    servletContextHandler.addServlet(holder, "/*");
    
    val handlerList = new HandlerCollection();
    handlerList.setHandlers( Array[Handler](contextHandler, contextHandler2, servletContextHandler, new DefaultHandler()));
    
    server.setHandler(handlerList);
    server.start()
  }
  
  def buildSwaggerUI(): ContextHandler = {
    val rh = new ResourceHandler()
    rh.setDirectoriesListed(true)
    rh.setResourceBase("./src/main/resources/api-docs")
    val context = new ContextHandler("/api-docs");
    context.setResourceBase("./src/main/resources/api-docs");
    context.setHandler(rh);
    context
  }
}

class MimirVizierServlet() extends HttpServlet with LazyLogging {
    def ellipsize(text: String, len: Int): String =
      if(text.size > len){ text.substring(0, len-3)+"..." } else { text }


    def process(
      handler: Request, 
      output: HttpServletResponse, 
    ){
      val response = 
        try {
          handler.handle
        } catch {
          case e: EOFException => 
            logger.error(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))
            ErrorResponse(
              e.getClass.getCanonicalName(),
              e.getMessage(), 
              e.getStackTrace.map(_.toString).mkString("\n")
            )

          case e: FileNotFoundException =>
            logger.error(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))
            ErrorResponse(
              e.getClass.getCanonicalName(),
              e.getMessage(), 
              e.getStackTrace.map(_.toString).mkString("\n")
            )

          case e: SQLException => {
            logger.error(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))
            ErrorResponse(
              e.getClass.getCanonicalName(),
              e.getMessage(), 
              e.getStackTrace.map(_.toString).mkString("\n")
            )
          }

          case e: AnnotationException => {
            logger.error(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))
            ErrorResponse(
              e.getClass.getCanonicalName(),
              e.getMessage(), 
              e.getStackTrace.map(_.toString).mkString("\n")
            )
          }
          case e: AnalysisException => {
            logger.error(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))
            ErrorResponse(
              e.getClass.getCanonicalName(),
              s"SQL Exception: ${e.getMessage}",
              e.getStackTrace.map(_.toString).mkString("\n")
            )                  
          }
          case FormattedError(errorResponse) => {
            logger.debug(s"Internally Formatted Error: ${errorResponse.errorType}")
            logger.error(s"Internally Formatted Error: ${errorResponse.errorType}" + 
                "\n" + errorResponse.errorMessage + 
                "\n" + errorResponse.stackTrace) 
            errorResponse
          }

          case e: Throwable => {
            logger.error("MimirAPI ERROR: ", e)
            ErrorResponse(
              e.getClass.getCanonicalName(),
              "An error occurred...", 
              s"""|${getThrowableMessage(e)}
                  |Caused by:
                  |${getThrowableMessage(Option(e.getCause).getOrElse(e))}"""
                  .stripMargin
            )
          }
        }

      response.write(output)
    }

    def processJson[Q <: Request](
      req: HttpServletRequest, 
      output: HttpServletResponse
    )(
      implicit format: Format[Q]
    ){
      val text = scala.io.Source.fromInputStream(req.getInputStream).mkString 
      try { 
        logger.debug(s"$text")
        process(Json.parse(text).as[Q], output)
      } catch {
        case e@JsResultException(errors) => {
          logger.error(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))
          ErrorResponse(
            e.getClass().getCanonicalName(),
            s"Error(s) parsing API request\n${ellipsize(text, 100)}\n"+stringifyJsonParseErrors(errors).mkString("\n"),
            e.getStackTrace.map(_.toString).mkString("\n")
          ).write(output)
        }
        case e:Throwable => {
          logger.error(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))
          ErrorResponse(
            e.getClass().getCanonicalName(),
            s"Error(s) parsing API request\n${ellipsize(text, 100)}\n",
            e.getStackTrace.map(_.toString).mkString("\n")
          ).write(output)
        }
      }

    }


    def fourOhFour(req: HttpServletRequest, output: HttpServletResponse)
    {
      output.setStatus(HttpServletResponse.SC_NOT_FOUND)
      logger.error(s"MimirAPI ${req.getMethod} Not Handled: ${req.getPathInfo}")
      ErrorResponse(
        s"MimirAPI ${req.getMethod} Not Handled: ${req.getPathInfo}",
        "Unknown Request:"+ req.getPathInfo, 
        Thread.currentThread().getStackTrace.map(_.toString).mkString("\n") 
      ).write(output)
    }

    val PREFIX = "\\/api\\/v2(\\/.*)".r

    override def doPost(req: HttpServletRequest, output: HttpServletResponse)
    {
      logger.info(s"MimirAPI POST ${req.getPathInfo}")
      req.getPathInfo match {
        case PREFIX(route) => 
          route match {
            case "/eval/scala"           => processJson[CodeEvalRequest](req, output)
            case "/eval/R"               => processJson[CodeEvalRequest](req, output)
            case "/dataSource/load"      => processJson[LoadRequest](req, output)
            case "/dataSource/inlined"   => processJson[LoadInlineRequest](req, output)
            case "/dataSource/unload"    => processJson[UnloadRequest](req, output)
            case "/lens/create"          => processJson[CreateLensRequest](req, output)
            case "/view/create"          => processJson[CreateViewRequest](req, output)
            case "/view/sample"          => processJson[CreateSampleRequest](req, output)
            case "/vizual/create"        => processJson[VizualRequest](req, output)
            case "/annotations/cell"     => processJson[ExplainCellRequest](req, output)
            case "/annotations/all"      => processJson[ExplainEverythingRequest](req, output)
            case "/query/data"           => processJson[QueryMimirRequest](req, output)
            case "/query/table"          => processJson[QueryTableRequest](req, output)
            case "/schema"               => processJson[SchemaForQueryRequest](req, output)
            case "/tableInfo"            => processJson[SchemaForTableRequest](req, output)
            case "/garbageCollect"       => process(GarbageCollectRequest(), output)
            case _                       => fourOhFour(req, output)
          }
        case _                           => fourOhFour(req, output)
      }
    }
    
    val HEAD = "\\/([^\\/]+)(/.*)".r
    val TAIL = "/([^\\/]+)".r

    override def doPut(req: HttpServletRequest, output: HttpServletResponse)
    {
      logger.info(s"MimirAPI PUT ${req.getPathInfo}")
      req.getPathInfo match {
        case PREFIX(route) => 
          route match {
            case HEAD("blob", TAIL(id))  => process(CreateBlobRequest(req, id), output)
            case "/blob"                 => process(CreateBlobRequest(req), output)
            case _                       => fourOhFour(req, output)
          }
        case _                           => fourOhFour(req, output)
      }
    }
    override def doGet(req: HttpServletRequest, output: HttpServletResponse) = {
      logger.info(s"MimirAPI GET ${req.getPathInfo}")
      req.getPathInfo match {
        case PREFIX(route) => 
          route match {
            case "/lens"                                    => LensList(Lenses.supportedLenses).write(output)
            case HEAD("blob", TAIL(id))                     => process(GetBlobRequest(id), output)
            case HEAD("tableInfo", TAIL(id))                => process(SchemaForTableRequest(id), output)
            case HEAD("tableInfo", HEAD(id, TAIL("schema")))=> process(SchemaForTableRequest(id), output)
            case HEAD("tableInfo", HEAD(id, TAIL("size")))  => process(SizeOfTableRequest(id), output)
            case _                                          => fourOhFour(req, output)
          }
        case _                                              => fourOhFour(req, output)
      }
    }

    def getThrowableMessage(e:Throwable):String = {
      s"""|${e.getMessage()}
          |${e.getStackTrace.mkString("\n")}""".stripMargin
    }
  }