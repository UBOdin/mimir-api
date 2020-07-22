package org.mimirdb.vizual

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification
import org.apache.spark.sql.types._

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.caveats.implicits._

class RegressionsSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll = {
    SharedSparkTestInstance.initAPI
    SharedSparkTestInstance.loadCSV("VIZUAL_REGRESSIONS_R", "test_data/r.csv", typeInference = false, header = true)
  }

  "Insert Row Type Matching" >> {
    val df = dataset("VIZUAL_REGRESSIONS_R")

    // sanity check before we start
    df.schema.fields.map { _.dataType }.toSeq should beEqualTo(Seq(StringType, StringType, StringType))

    // actually check if insertion breaks types
    val result = ExecOnSpark.apply(df, InsertRow(2))

    result.schema.fields.map { _.dataType }.toSeq should beEqualTo(Seq(StringType, StringType, StringType))

  }
}
