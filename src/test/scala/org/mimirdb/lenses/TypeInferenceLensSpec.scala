package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification
import org.apache.spark.sql.types._

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.caveats.implicits._

class TypeInferenceLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll = {
    SharedSparkTestInstance.initAPI
    SharedSparkTestInstance.loadCSV("TI_ERROR_R", "test_data/error_r.csv", typeInference = true)
  }

  "Type Inference Lens" >> {
    val lens = Lenses(Lenses.typeInference)
    val df = dataset("TI_ERROR_R")
    val config = lens.train(df, JsNull)

    val result = lens.apply(df, config, "CONTEXT_CUE")
    result.count() must beEqualTo(7)
    result.schema.fields.toSeq.map { _.dataType } must beEqualTo(
      Seq(ShortType, ShortType, ShortType)
    )

    result.listCaveats(true, Set("B")) must haveSize(1)
  }
}
