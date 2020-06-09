package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification
import org.apache.spark.sql.types._

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.implementation.RepairKeyLensConfig


class RepairKeyLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll = SharedSparkTestInstance.initAPI

  "Repair Key Lens Without Weights" >> {
    val lens = Lenses("REPAIR_KEY")
    val df = dataset("TEST_R")
    val config = lens.train(df, Json.toJson(RepairKeyLensConfig("A")))

    val result = lens.apply(df, config, "REPAIR_A")
    result.count() must beEqualTo(3)

    val caveats = result.listCaveats()
    // println(caveats.map { _.message }.mkString("\n"))

    // < A:1 > has 3 different possible values for both B and C.
    // < A:4 > has only one row
    // < A:2 > has 2 possible values for B, but one is null
    //         and 1 possible value across 2 rows for C.
    caveats.size must beEqualTo(2)
  }

  "Web UI Issue 148" >> {
    // https://github.com/VizierDB/web-ui/issues/148

    SharedSparkTestInstance.loadCSV("ISSUE_148", "test_data/web_ui_147.csv")
    val lens = Lenses("REPAIR_KEY")
    val df = dataset("ISSUE_148")
    val config = lens.train(df, Json.toJson(RepairKeyLensConfig("A")))

    val result = lens.apply(df, config, "REPAIR_A")

    result.count() must beEqualTo(5)
  }
}
