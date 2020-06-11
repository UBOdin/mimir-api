package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.implementation.MergeAttributesLensConfig


class MergeAttributesLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll = SharedSparkTestInstance.initAPI

  "Merge Attributes Lens" >> {
    val lens = Lenses("PICKER")
    val df = dataset("TEST_R")
    val config = lens.train(df, 
      Json.toJson(
        MergeAttributesLensConfig(
          inputs = Seq("A", "B"),
          output = "A",
          dataType = None
        )
      )
    )

    val result = lens.apply(df, config, "CONTEXT_CUE")
    result.count() must beEqualTo(7)
    val caveats = result.listCaveats()
    caveats.size must beEqualTo(6)

  }
}
