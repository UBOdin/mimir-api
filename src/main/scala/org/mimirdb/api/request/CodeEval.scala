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
import scala.collection.mutable.Buffer

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
      val result = Eval[OutputBuffer](
        Eval.STANDARD_PREFIX + source + Eval.STANDARD_SUFFIX
      )
      CodeEvalResponse(
        result.stdout.map { _.toString }.mkString("\n"),
        result.stderr.map { _.toString }.mkString("\n")
      )
    } catch {
      case t: Throwable => {
        //logger.error(s"Error Evaluating Scala Source", t)
        CodeEvalResponse("",s"Error Evaluating Scala Source: \n${t.getMessage()}\n${t.getStackTrace.mkString("\n")}\n${t.getMessage()}\n${Option(t.getCause).getOrElse(t).getStackTrace.mkString("\n")}")
      }
    }
  }
}

object Eval {

  val STANDARD_PREFIX = """
    import org.mimirdb.api.request.{ VizierDB, OutputBuffer }
    val mimir_output_buffer = new OutputBuffer
    import mimir_output_buffer.{print, println}
  """

  val STANDARD_SUFFIX = """
    mimir_output_buffer
  """
  
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

class OutputBuffer {
  val stderr = Buffer[Output]()
  val stdout = Buffer[Output]()

  def print(str: String, err:Boolean = false ) =
    if(err) { stderr.append(StringOutput(str)) }
    else    { stdout.append(StringOutput(str)) }
  def println(str: String, err: Boolean = false) =
    if(str.size == 0){ print("\n", err) }
    else if(str(str.size-1) == '\n'){ print(str, err) }
    else { print(str+"\n", err) }
}

sealed trait Output
case class StringOutput(str: String) extends Output
  { override def toString() = str }

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
