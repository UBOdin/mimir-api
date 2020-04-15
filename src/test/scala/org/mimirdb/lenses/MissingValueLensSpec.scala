package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.lenses.implementation.MissingValueLensConfig
import org.mimirdb.api.request.CreateLensRequest
import org.mimirdb.api.request.CreateLensResponse


class MissingValueLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll = SharedSparkTestInstance.initAPI

  "Missing Value Lens" >> {
    val missingValue = Lenses("MISSING_VALUE")
    val df = dataset("TEST_R")
    
    val request = CreateLensRequest(
                      "TEST_R",
                      JsNull, 
                      "TYPE_INFERENCE",
                      false,
                      Some("A TEST"),
                      None,
                    )
    val response = request.handle.as[CreateLensResponse]
    val tidf = MimirAPI.catalog.get(response.lensName)
  
    val config = missingValue.train(tidf, JsArray(IndexedSeq(JsString("B"),JsString("C"))))
    val parsedConfig = config.as[MissingValueLensConfig]
    
    val result = missingValue.apply(tidf, config, "CONTEXT")
    result.count() must beEqualTo(7)
    result.collect().apply(2).getShort(1) must beEqualTo(2)
    result.collect().apply(3).getShort(2) must beEqualTo(2)
  }
}
