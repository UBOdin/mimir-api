package org.mimirdb.api.request

import org.apache.spark.sql.SparkSession
import play.api.libs.json._

import org.mimirdb.api.{ Request, Response }
import org.mimirdb.api.MimirAPI


case class CodeEvalRequest (
            /* scala source code to evaluate*/
                  input: Map[String,String],
                  language: String,
                  source: String
) extends Request {
  def handle = {
    Json.toJson(
      language match {
        case "R"     => evalR(source)
        case "scala" => evalScala(source)
      }
    )
  }

  def evalR(source: String): CodeEvalResponse = ???

  def evalScala(source: String): CodeEvalResponse = ???
}

object CodeEvalRequest {
  implicit val format: Format[CodeEvalRequest] = Json.format
}


case class CodeEvalResponse (
            /* stdout from evaluation of scala code */
                  stdout: String,
            /* stderr from evaluation of scala code */
                  stderr: String
) extends Response

object CodeEvalResponse {
  implicit val format: Format[CodeEvalResponse] = Json.format
}
