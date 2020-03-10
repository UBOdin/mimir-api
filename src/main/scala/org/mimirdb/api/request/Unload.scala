package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.SparkSession
import java.io.File

import org.mimirdb.api.{ Request, Response, Tuple }
import org.mimirdb.api.MimirAPI



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
    Json.toJson(UnloadResponse(
      unload(
        input,
        file,
        format,
        backendOption.map { tup => tup.name -> tup.value } 
      ).map { _.toString }
    ))

  def unload(
    sourceTable: String,
    url: String,
    format: String,
    sparkOptions: Seq[(String,String)],
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): Seq[File] = {
    ???
  }
}

object UnloadRequest {
  implicit val format: Format[UnloadRequest] = Json.format
}

case class UnloadResponse (
            /* name of resulting table */
                  outputFiles: Seq[String]
) extends Response

object UnloadResponse {
  implicit val format: Format[UnloadResponse] = Json.format
}
