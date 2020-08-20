package org.mimirdb.spark

import java.io.ByteArrayInputStream
import java.util.Base64
import scala.sys.process._
import com.typesafe.scalalogging.LazyLogging

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{ Expression, PythonUDF }
import org.apache.spark.PythonUDFWorkaround // defined in mimir-api

class PythonUDFBuilder(pythonPath: String)
  extends LazyLogging
{

  def apply(vizierFunctionScript: String): (Seq[Expression] => PythonUDF) = 
    apply(pickle(vizierFunctionScript))

  def apply(pickled: Array[Byte]): (Seq[Expression] => PythonUDF) = 
  {
    return (args: Seq[Expression]) => PythonUDFWorkaround(
      command = pickled,
      envVars = new java.util.HashMap(),
      pythonIncludes = new java.util.ArrayList(),
      pythonExec = pythonPath,
      pythonVer = version
    )(
      name = "TEST_FUNCTION",
      dataType = IntegerType,
      children = args,
      evalType = 100,
      true
    )
  }

  def pickle(vizierFunctionScript: String): Array[Byte] = 
    Base64.getDecoder().decode(
      python(GENERATE_PICKLE(vizierFunctionScript))
        .replaceAll("\n", "")
    )

  def runPickle(pickled: Array[Byte], args: String = ""): String = 
    python(RUN_PICKLE(pickled, args))

  def python(script: String): String = 
  {
    logger.debug(s"Running:\n$script")
    Process(pythonPath) .#< {  new ByteArrayInputStream(script.getBytes()) }  .!!
  }

  lazy val version = 
    Process(Seq(pythonPath, "--version")).!!
      .replaceAll("\n", "")
      .split(" ").reverse.head
      .split("\\.").take(2).mkString(".")


  def GENERATE_PICKLE(vizier_fn: String) = s"""
import cloudpickle
import sys
import base64
from pyspark.sql.types import IntegralType

class VizierUDFExtractor:
  def __init__(self):
    self.fn = None
  def export_module_decorator(self, fn):
    self.fn = fn
    return fn
vizierdb = VizierUDFExtractor()

${vizier_fn}

assert(vizierdb.fn is not None)
pickled_fn = cloudpickle.dumps((vizierdb.fn, IntegralType()))
encoded_fn = base64.encodebytes(pickled_fn)
print(encoded_fn.decode())
"""

  def RUN_PICKLE(pickled: Array[Byte], args: String) = s"""
import cloudpickle
import base64

encoded_fn = "${new String(Base64.getEncoder().encode(pickled)).replaceAll("\n", "")}"
pickled_fn = base64.decodebytes(encoded_fn.encode())
fn = cloudpickle.loads(pickled_fn)[0]
print(fn(${args}))
"""
}