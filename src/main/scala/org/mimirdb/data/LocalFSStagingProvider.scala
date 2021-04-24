package org.mimirdb.data

import java.net.URL
import java.io.{ File, InputStream, OutputStream, FileOutputStream }
import java.sql.SQLException
import scala.util.Random
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.mimirdb.api.request.VizierDB
import org.mimirdb.api.MimirAPI

/**
 * RawFileProvider backed by the local filesystem.
 * @param basePath  The path to stage files to (defaults to the current working directory)
 */
class LocalFSStagingProvider(
  basePath: File = new File("."),
  basePathIsRelativeToDataDir: Boolean = false
) extends StagingProvider
    with LazyLogging
{
  def this(basePath: String, basePathIsRelativeToDataDir: Boolean) = 
    this(new File(basePath), basePathIsRelativeToDataDir)

  val absoluteBasePath: File = 
    if(basePathIsRelativeToDataDir) {
      MimirAPI.conf.resolveToDataDir(basePath.toString)
    } else { basePath }

  /** 
   * Randomly allocate a name relative to basePath (internal only)
   * @param extension   The file extension to use for the allocated name
   * @param nameHint    A hint that will be part of the name
   * @return            The absolute path to a guaranteed unique file with the 
   *                    specified extension and the path relative to basePath
   */
  private def makeName(extension: String, nameHint: Option[String]): (File, String) =
  {
    val rand = new Random().alphanumeric
    // Try 1000 times to create a randomly named file
    for(i <- 0 until 1000){
      val candidate =
        nameHint match { 
          case Some(hint) => s"${hint.replaceAll("[^a-zA-Z0-9]", "")}-${rand.take(10).mkString}.${extension}"
          case None => s"${rand.take(20).mkString}.${extension}"
        }
      val absolute = new File(absoluteBasePath, candidate)
      // If the randomly named file doesn't exist, we're done.
      if(!absolute.exists()){ 
        return (
          absolute, 
          new File(basePath, candidate).toString
        ) 
      }
    }
    // Fail after 1000 attempts.
    throw new SQLException(s"Can't allocate name for $nameHint")
  }

  /**
   * Transfer an InputStream to an OutputStream
   * @param input     The InputStream to read from
   * @param output    The OutputStream to write to
   *
   * Obsoleted in Java 9 with InputStream.transferTo... but we're on 8 for now
   */
  private def transferBytes(input: InputStream, output: OutputStream): Unit =
  {
    val buffer = Array.ofDim[Byte](1024*1024) // 1MB buffer
    var bytesRead = input.read(buffer)
    while(bytesRead >= 0) { 
      output.write(buffer, 0, bytesRead)
      bytesRead = input.read(buffer)
    }
  }

  def ensureBasePath
  {
    if(!basePath.exists){ basePath.mkdirs }
  }

  def stage(input: InputStream, fileExtension: String, nameHint: Option[String]): (String, Boolean) = 
  {
    ensureBasePath
    val (file, relative) = makeName(fileExtension, nameHint)
    transferBytes(input, new FileOutputStream(file))
    return (relative, basePathIsRelativeToDataDir)
  }
  def stage(url: URL, nameHint: Option[String]): (String, Boolean) =
  {
    ensureBasePath
    val pathComponents = url.getPath.split("/")
    val nameComponents = pathComponents.reverse.head.split("\\.")
    val extension = 
      if(nameComponents.size > 1) { nameComponents.reverse.head }
      else { "data" } // default to generic 'data' if there's no extension
    stage(url.openStream(), extension, nameHint)
  }
  def stage(input: DataFrame, format: String, nameHint:Option[String]): (String, Boolean) =
  {
    ensureBasePath
    val (file, relative) = makeName(format, nameHint)
    input.write
         .format(format)
         .save(file.toString)
    return (relative, basePathIsRelativeToDataDir)
  }
  def drop(local: String): Unit = 
  {
    val actual = if(basePathIsRelativeToDataDir){ 
                   MimirAPI.conf.resolveToDataDir(local)
                 } else { new File(local) }
    if(actual.exists()){
      actual.delete()
    }
  }
}