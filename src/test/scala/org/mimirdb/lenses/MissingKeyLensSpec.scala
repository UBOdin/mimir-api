package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.lenses.implementation.MissingKeyLensConfig


class MissingKeyLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll = SharedSparkTestInstance.initAPI

  "Missing Key Lens" >> {
    val missingKey = Lenses("MISSING_KEY")
    val df = dataset("SEQ")
    val config = missingKey.train(df, JsString("KEY"))
    val parsedConfig = config.as[MissingKeyLensConfig]
    
    parsedConfig.low must beEqualTo(1l)
    parsedConfig.high must beEqualTo(10l+1)
    parsedConfig.step must beEqualTo(1l)

    val result = missingKey.apply(df, config, "CONTEXT")

    result.count() must beEqualTo(10)
  }
}