package org.mimirdb.spark

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification
import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.apache.spark.sql.types.StringType
import org.mimirdb.api.request.CreateViewRequest

class PythonUDFSpec
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  def beforeAll = SharedSparkTestInstance.initAPI

  val ADD_ONE = """
from pyspark.sql.types import StringType, IntegerType
@vizierdb.export_module_decorator
@return_type(IntegerType())
def add_one(x):
  return int(x) + 1
"""

  "Pickle Python UDFs" >> {

    println(s"Using ${MimirAPI.pythonUDF.pythonPath}")
    MimirAPI.pythonUDF.version.split("\\.")(0) must beEqualTo("3")

    val pickled: Array[Byte] = MimirAPI.pythonUDF.pickle(ADD_ONE)

    pickled.size must beGreaterThan(0)

    MimirAPI.pythonUDF.runPickle(pickled, "1")
             .replaceAll("\n", "")
             .toInt must beEqualTo(2)

    def addOne(x: Column) = new Column(MimirAPI.pythonUDF(pickled)(Seq(x.expr)))
    val df = 
      spark.range(100)
           .select(col("id"), addOne(col("id")).as("plus_one"))

    val addOneResults = df.collect()
    addOneResults.size must beEqualTo(100)
    addOneResults.map { row =>
                   row.getAs[Int]("plus_one").toInt must beEqualTo(
                     row.getAs[Long]("id") + 1
                   )
                 }
    ok
  }

  "Invoke Blobbed Functions" >>
  {
    MimirAPI.blobs.put("ADD_ONE", "application/python", ADD_ONE.getBytes())

    val response = CreateViewRequest(
      input = Map("test" -> "SEQ"),
      functions = Some(Map("do_a_thing" -> "ADD_ONE")),
      query = """
      SELECT key, do_a_thing(key) as plus_one FROM TEST
      """,
      resultName = Some("PYTHON_FN_RESULT"),
      properties = Some(Map())
    ).handle

    val df = MimirAPI.catalog.get(response.name)
    df.show()

    val addOneResults = df.collect()
    addOneResults.size must beEqualTo(8)
    addOneResults.map { row =>
                   row.getAs[Int]("plus_one").toInt must beEqualTo(
                     row.getAs[String]("key").toInt + 1
                   )
                 }
    ok


  }

}