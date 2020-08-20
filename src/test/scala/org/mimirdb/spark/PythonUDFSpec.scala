package org.mimirdb.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification
import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }

class PythonUDFSpec
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  def beforeAll = SharedSparkTestInstance.initAPI

  val pythonUDF = new PythonUDFBuilder("/home/okennedy/.pyenv/shims/python3")

  val ADD_ONE = """
@vizierdb.export_module_decorator
def add_one(x):
  return x + 1
"""

  "Pickle Python UDFs" >> {

    pythonUDF.version must beEqualTo("3.8")

    val pickled: Array[Byte] = pythonUDF.pickle(ADD_ONE)

    pickled.size must beGreaterThan(0)

    val ret: String = pythonUDF.runPickle(pickled, "1").replaceAll("\n", "")
    ret.toInt must beEqualTo(2)

    def addOne(x: Column) = new Column(pythonUDF(pickled)(Seq(x.expr)))
    val df = 
      spark.range(100)
           .select(col("id"), addOne(col("id")))
           .show()

    ok
  }

}