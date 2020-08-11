package org.mimirdb.api.request

import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.api.CreateResponse

class CreateSampleSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  "CreateSample" >> {
    "Uniform" >> {
      val request = CreateSampleRequest(
                      "TEST_R",
                      Sample.Uniform(0.5),
                      Some(42l),
                      None,
                      None
                    )
      val response = Json.toJson(request.handle).as[CreateResponse]
      val df = MimirAPI.catalog.get(response.name)
      df.count() must be beCloseTo(3, /*+/-*/ 2)
    }
    "Stratified" >> {
      val request = CreateSampleRequest(
                      "TEST_R",
                      Sample.StratifiedOn("A", Seq[(JsValue, Double)](
                        JsString("1") -> 0.5,
                        JsString("2") -> 1.0,
                        JsString("3") -> 1.0
                      )),
                      Some(42l),
                      None,
                      None
                    )
      val response = Json.toJson(request.handle).as[CreateResponse]
      val df = MimirAPI.catalog.get(response.name)
      df.filter { df("A") === "1" }.count() must be beCloseTo(2, /*+/-*/ 1)
      df.filter { df("A") === "2" }.count() must be equalTo(2)
      df.filter { df("A") === "3" }.count() must be equalTo(0)
      df.filter { df("A") === "4" }.count() must be equalTo(0)
    }
  }

}