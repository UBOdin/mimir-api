package org.mimirdb.spark

import java.io.ByteArrayInputStream
import java.util.Base64
import scala.sys.process._

import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification
import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }

class PythonUDFSpec
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  def beforeAll = SharedSparkTestInstance.initAPI

  def EXTRACT_PICKLE(vizier_fn: String) = s"""
import cloudpickle
import sys
import base64

class VizierUDFExtractor:
  def __init__(self):
    self.fn = None
  def export_module_decorator(self, fn):
    self.fn = fn
    return fn
vizierdb = VizierUDFExtractor()

${vizier_fn}

assert(vizierdb.fn is not None)
pickled_fn = cloudpickle.dumps(vizierdb.fn)
encoded_fn = base64.encodebytes(pickled_fn)
print(encoded_fn.decode())
"""

  def RUN_PICKLE(pickled: String, args: String) = s"""
import cloudpickle
import base64

encoded_fn = "${pickled.replaceAll("\n", "")}"
pickled_fn = base64.decodebytes(encoded_fn.encode())
fn = cloudpickle.loads(pickled_fn)
print(fn(${args}))
"""

  val ADD_ONE = """
@vizierdb.export_module_decorator
def add_one(x):
  return x + 1
"""

  val PYTHON="/home/okennedy/.pyenv/shims/python3"

  def python(script: String): String = 
    Process(PYTHON) .#< {  new ByteArrayInputStream(script.getBytes()) }  .!!

  "Pickle Python UDFs" >> {
    val pickledBase64: String = python(EXTRACT_PICKLE(ADD_ONE))  
    val pickled = Base64.getDecoder().decode(pickledBase64.replaceAll("\n", ""))

    pickled.size must beGreaterThan(0)

    val ret: String = python(RUN_PICKLE(pickledBase64, "1")).replaceAll("\n", "")
    print(ret)
    ret.toInt must beEqualTo(2)
    ok
  }

}