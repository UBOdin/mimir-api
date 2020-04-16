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

object MimirAPI extends LazyLogging {
  
  var isRunning = true
  val DEFAULT_API_PORT = 8089

  var sparkSession: SparkSession = null
  var catalog: Catalog = null
  var server: Server = null
  var conf: MimirConfig = null

  def main(args: Array[String])
  {
    conf = new MimirConfig(args);
    conf.verify

    // Initialize Spark
    sparkSession = InitSpark.local

    // Initialize the catalog
    { 
      val metadata = conf.metadata().split(":").toList match {
        case "sqlite" :: Nil => 
          new JDBCMetadataBackend("sqlite", s"${conf.dataDir()}vizier.db")
        case "sqlite" :: rest => 
          new JDBCMetadataBackend("sqlite", rest.mkString(":"))
        case _ => throw new IllegalArgumentException(s"Unknown metadata provider: ${conf.metadata}")
      }
      val staging = new LocalFSStagingProvider(conf.staging())
      catalog = new Catalog(metadata, staging, sparkSession)
    }

    // Initialize Geocoders (if configuration options available)
    val geocoders = 
      Seq(
        conf.googleAPIKey.map { new GoogleGeocoder(_) }.toOption,
        conf.osmServer.map { new OSMGeocoder(_) }.toOption
      ).flatten
    if(!geocoders.isEmpty){ Lenses.initGeocoding(geocoders, catalog) }

    //populate spark after lens initialization
    catalog.populateSpark
    
    // Start the server
    runServer(conf.port())

    //sparkSession.conf.set( "spark.sql.optimizer.excludedRules", "org.apache.spark.sql.catalyst.optimizer.CollapseProject")
    //println(Query.apply("SELECT TKEY,A,B FROM LENS_MISSING_VALUE_521717238", true, sparkSession))
    
    // And sleep until done
    println(s"Mimir API Server Started on http://localhost:${conf.port()}/...")
     while(isRunning){
       Thread.sleep(90000)
       
     }
     Thread.sleep(1000)
     server.stop();
  }
  
  def runServer(port: Int = DEFAULT_API_PORT) : Unit = {
    if(server != null){ 
      throw new RuntimeException("Can't have two Mimir servers running in one JVM")
    }
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
    override def doPost(req : HttpServletRequest, resp : HttpServletResponse) = {
        val text = scala.io.Source.fromInputStream(req.getInputStream).mkString 
        println(s"MimirAPI POST ${req.getPathInfo}\n$text")
        val routePattern = "\\/api\\/v2(\\/[a-zA-Z\\/]+)".r
        val os = resp.getOutputStream()
        resp.setHeader("Content-type", "text/json");
        val response:JsValue = 
          req.getPathInfo match {
            case routePattern(route) => {
              try{
                val input:JsValue = Json.parse(text)
                val handler:Request = 
                  route match {
                    case "/eval/scala"           => input.as[CodeEvalRequest]
                    case "/eval/R"               => input.as[CodeEvalRequest]
                    case "/dataSource/load"      => input.as[LoadRequest]
                    case "/dataSource/unload"    => input.as[UnloadRequest]
                    case "/lens/create"          => input.as[CreateLensRequest]
                    case "/view/create"          => input.as[CreateViewRequest]
                    case "/view/sample"          => input.as[CreateSampleRequest]
                    case "/annotations/cell"     => input.as[ExplainCellRequest]
                    case "/annotations/all"      => input.as[ExplainEverythingRequest]
                    case "/query/data"           => input.as[QueryMimirRequest]
                    case "/schema"               => input.as[SchemaForQueryRequest]
                    case "/annotations/feedback" => {
                      throw new UnsupportedOperationException("Feedback No Longer Supported")
                    }
                    case "/adaptive/create" => {
                      throw new UnsupportedOperationException("Adaptive Schemas No Longer Exist")
                    }
                  }
                  handler.handle
              } catch {
                case e: EOFException => 
                  Json.toJson(ErrorResponse(
                    e.getClass.getCanonicalName(),
                    e.getMessage(), 
                    e.getStackTrace.map(_.toString).mkString("\n")
                  ))
        
                case e: FileNotFoundException =>
                  Json.toJson(ErrorResponse(
                    e.getClass.getCanonicalName(),
                    e.getMessage(), 
                    e.getStackTrace.map(_.toString).mkString("\n")
                  ))
        
                case e: SQLException => {
                  logger.debug(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))
                  Json.toJson(ErrorResponse(
                    e.getClass.getCanonicalName(),
                    e.getMessage(), 
                    e.getStackTrace.map(_.toString).mkString("\n")
                  ))
                }
        
                case e: AnnotationException => {
                  logger.debug(e.getMessage + "\n" + e.getStackTrace.map(_.toString).mkString("\n"))
                  Json.toJson(ErrorResponse(
                    e.getClass.getCanonicalName(),
                    e.getMessage(), 
                    e.getStackTrace.map(_.toString).mkString("\n")
                  ))
                }
        
                case e: Throwable => {
                  logger.error("MimirAPI POST ERROR: ", e)
                  Json.toJson(ErrorResponse(
                    e.getClass.getCanonicalName(),
                    "An unknown error occurred...", 
                    e.getStackTrace.map(_.toString).mkString("\n")
                  ))
                }
              }  
            }
            case _ => {
              logger.error(s"MimirAPI POST Not Handled: ${req.getPathInfo}")
              Json.toJson(ErrorResponse(
                "MimirAPI POST Not Handled",
                "Unknown Request:"+ req.getPathInfo, 
                Thread.currentThread().getStackTrace.map(_.toString).mkString("\n") 
              ))
            }
          } 
        os.write(Json.stringify(response).getBytes)
        os.flush()
        os.close() 
    }
    override def doGet(req : HttpServletRequest, resp : HttpServletResponse) = {
      println(s"MimirAPI GET ${req.getPathInfo}")
        
      val routePattern = "\\/api\\/v2(\\/[a-zA-Z\\/]+)".r
        req.getPathInfo match {
          case routePattern(route) => {
            try{
              val os = resp.getOutputStream()
              resp.setHeader("Content-type", "text/json");
              val response = 
                route match {
                  case "/lens" => {
                    Json.toJson(LensList(Lenses.supportedLenses))
                  }
                  case "/adaptive" => {
                    throw new UnsupportedOperationException("Adaptive Schemas No Longer Exist")
                  }
                }
              os.write(Json.stringify(response).getBytes)
              os.flush()
              os.close() 
            } catch {
              case t: Throwable => {
                logger.error("MimirAPI GET ERROR: ", t)
                throw t
              }
            }
          }
          case _ => {
            logger.error(s"MimirAPI GET Not Handled: ${req.getPathInfo}")
            throw new Exception("request Not handled: " + req.getPathInfo)
          }
        }  
    }
  }