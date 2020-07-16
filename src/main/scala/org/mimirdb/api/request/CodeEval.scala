package org.mimirdb.api.request

import org.apache.spark.sql.SparkSession
import play.api.libs.json._

import org.mimirdb.api.{ Request, JsonResponse }
import org.mimirdb.api.MimirAPI
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import scala.tools.reflect.ToolBox
import scala.reflect.runtime.currentMirror
import java.io.File

case class CodeEvalRequest (
            /* scala source code to evaluate*/
                  input: Map[String,String],
                  language: String,
                  source: String
) extends Request {
  def handle = {
    language match {
      case "R"     => evalR(source)
      case "scala" => evalScala(source)
    }
  }

  def evalR(source: String): CodeEvalResponse = ???

  //def evalScala(source: String): CodeEvalResponse = ???
  
  def evalScala(source : String) : CodeEvalResponse = {
    try {
      CodeEvalResponse(Eval("import org.mimirdb.api.request.VizierDB\n"+source),"")
    } catch {
      case t: Throwable => {
        //logger.error(s"Error Evaluating Scala Source", t)
        CodeEvalResponse("",s"Error Evaluating Scala Source: \n${t.getMessage()}\n${t.getStackTrace.mkString("\n")}\n${t.getMessage()}\n${Option(t.getCause).getOrElse(t).getStackTrace.mkString("\n")}")
      }
    }
  }
}

object Eval {
  
    def apply[A](string: String): A = {
      val toolbox = currentMirror.mkToolBox()
      val tree = toolbox.parse(string)
      toolbox.eval(tree).asInstanceOf[A]
    }
  
    def fromFile[A](file: File): A =
      apply(scala.io.Source.fromFile(file).mkString(""))
  
    def fromFileName[A](file: String): A =
      fromFile(new File(file))
  
  }
  
object VizierDB {
  
  def setHadoopConfig(key:String, value:String) = {
    MimirAPI.sparkSession.sparkContext.hadoopConfiguration.set(key,value)
  }
  
  def withDataset[T](dsname:String, handler: Dataset[Row] => T) : T = {
    MimirAPI.catalog.populateSpark()
    handler(MimirAPI.catalog.get(dsname))
  }
      
  def outputAnnotations(dsname:String) : String = {
    Explain(s"SELECT * FROM $dsname").map(_.toString).mkString("<br>")
  }
  
}

object CodeEvalRequest {
  implicit val format: Format[CodeEvalRequest] = Json.format
}


case class CodeEvalResponse (
            /* stdout from evaluation of scala code */
                  stdout: String,
            /* stderr from evaluation of scala code */
                  stderr: String
) extends JsonResponse[CodeEvalResponse]

object CodeEvalResponse {
  implicit val format: Format[CodeEvalResponse] = Json.format
}
