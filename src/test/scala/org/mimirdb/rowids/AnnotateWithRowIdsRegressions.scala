package org.mimirdb.rowids

import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.implementation.{ RepairKeyLens, RepairKeyLensConfig }
import org.mimirdb.api.request.Explain
import org.mimirdb.api.request.CreateLensRequest
import org.mimirdb.api.request.CreateViewRequest
import org.mimirdb.caveats.EnumerableCaveatSet
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.catalyst.encoders.RowEncoder

class AnnotateWithRowIdsRegressions
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  def beforeAll(): Unit = SharedSparkTestInstance.initAPI

  def trackReferences(e: org.apache.spark.sql.catalyst.expressions.Expression, prefix: String =  "  ")
  {
    println(s"\n$prefix$e -> ${e.references}")
    e match {
      case org.apache.spark.sql.catalyst.expressions.Exists(q, _, _) => println(q)
      case _ => ()
    }
    for(child <- e.children) { 
      trackReferences(child, prefix + "  ")
    }
  }

  "Left Outer Join Caveat Expansion" >> 
  {

    SharedSparkTestInstance.loadCSV("ROWID_REGRESSION_R", "test_data/r.csv", typeInference = false)
    SharedSparkTestInstance.loadCSV("ROWID_S", "test_data/s.csv", typeInference = false)

    CreateLensRequest(
      input = "ROWID_REGRESSION_R",
      params = Json.toJson(RepairKeyLensConfig("A", None)),
      `type` = "repair_key",
      humanReadableName = Some("R.csv"),
      materialize = false,
      resultName = Some("ROWID_R"),
      properties = None
    ).handle
    // CreateLensRequest(
    //   input = "ROWID_REGRESSION_S",
    //   params = Json.toJson(RepairKeyLensConfig("__2", None)),
    //   `type` = "repair_key",
    //   humanReadableName = Some("S.csv"),
    //   materialize = false,
    //   resultName = Some("ROWID_S"),
    //   properties = None
    // ).handle

    CreateViewRequest(
      input = Map(
        "ROWID_R" -> "ROWID_R",
        "ROWID_S" -> "ROWID_S"
      ),
      functions = None,
      query = """SELECT r.a, r.b, r.c, s1.__1 as x, s2.__1 as y 
                |FROM ROWID_R r
                |LEFT OUTER JOIN ROWID_S s1 on r.A = s1.__2
                |LEFT OUTER JOIN ROWID_S s2 on r.A = s2.__2""".stripMargin,
      resultName = Some("ROWID_JOINT"),
      properties = None
    ).handle

    val joint = AnnotateWithRowIds(MimirAPI.catalog.get("ROWID_JOINT"))


    {
      val rowid = joint.where("A = 1").take(1)(0).getAs[Long](AnnotateWithRowIds.ATTRIBUTE)
      Explain(
        "SELECT * FROM ROWID_JOINT",
        rows = Seq(rowid.toString),
        cols = Seq("B")
      ) must not beEmpty
    }
  }

}