package org.mimirdb.api.request

import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI


class CreateLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  "CreateLens" >> {
    "TYPE_INFERENCE" >> {
      val request = CreateLensRequest(
                      "TEST_R",
                      JsNull, 
                      "TYPE_INFERENCE",
                      false,
                      Some("A TEST"),
                      None,
                    )
      val response = request.handle.as[CreateLensResponse]
      val df = MimirAPI.catalog.get(response.lensName)
      val row = df.take(1)(0)
      row(row.fieldIndex("A")).asInstanceOf[AnyRef] must not(beAnInstanceOf[String])

    }
  }

}