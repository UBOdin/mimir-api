package org.mimirdb.api.request

import play.api.libs.json._
import org.apache.spark.sql.SparkSession
import java.io.File
import com.typesafe.scalalogging.LazyLogging

import org.mimirdb.api.{ Request, Response, Tuple }
import org.mimirdb.api.MimirAPI
import org.mimirdb.data.FileFormat
import org.mimirdb.util.TimerUtils

case class UnloadRequest (
            /* table or view to unload */
                  input: String,
            /* file url of datasorce to unload */
                  file: String,
            /* format of file for spark */
                  format: String,
            /* options for spark datasource api */
                  backendOption: Seq[Tuple]
) 
  extends Request 
  with LazyLogging 
  with TimerUtils 
{
  def handle = {
    val sparkOptions = backendOption.map { tup => tup.name -> tup.value } 

    val df = MimirAPI.catalog.get(input)

    val writer = 
      backendOption.foldLeft(
        df.write.format(format) 
      ) { 
        case (writer, Tuple("mode", mode))  => writer.mode(mode)
        case (writer, Tuple(option, value)) => writer.option(option, value) 
      }

    val fileIsEmpty = (file == null) || (file.isEmpty)

    if(fileIsEmpty){ writer.save }
    else {
      // Some formats need special handling to reformat their URLs
      format match {
        case FileFormat.GOOGLE_SHEETS => {
          // The Sheets uploader doesn't take a full sheet URL, just the 
          // spreadsheetID/sheetID pair at the end of the URL.  Srip those
          // out and recreate the URL. 
          val sheetURLParts = file.split("\\/").reverse
          // NOTE THE `.reverse` above
          val sheetIdentifier = sheetURLParts(0) + "/" + sheetURLParts(1)
          writer.save(sheetIdentifier)
        }
        case _ => writer.save(file)
      }
    }

    val outputFiles: Seq[String] = 
      if(fileIsEmpty){ Seq[String]() }
      else {
        val filedir = new File(file)
        filedir.listFiles.filter(_.isFile)
          .map(_.getName).toSeq
      }

    Json.toJson(UnloadResponse(outputFiles))
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
