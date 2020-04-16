package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.lenses.implementation.MissingValueLensConfig
import org.mimirdb.api.request.CreateLensRequest
import org.mimirdb.api.request.CreateLensResponse
import org.mimirdb.api.request.Query


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
    val mvconfigDefault = JsArray(IndexedSeq(JsString("B")/*,JsString("C")*/))
    val requestMV = CreateLensRequest(
                      response.lensName,
                      mvconfigDefault, 
                      "MISSING_VALUE",
                      false,
                      Some("A TEST"),
                      None,
                    )
    val responseMV = requestMV.handle.as[CreateLensResponse]
    val result = Query(s"SELECT * FROM ${responseMV.lensName}",true).data
    result.length must beEqualTo(7)
    result(2)(1) must beEqualTo(2)
    //result(3)(2) must beEqualTo(2)
    
  }
}
