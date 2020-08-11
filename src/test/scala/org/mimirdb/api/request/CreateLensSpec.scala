package org.mimirdb.api.request

import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.lenses.Lenses


class CreateLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  "CreateLens" >> {
    Lenses.typeInference >> {
      val request = CreateLensRequest(
                      "TEST_R",
                      JsNull, 
                      Lenses.typeInference,
                      false,
                      Some("A TEST"),
                      None,
                      None
                    )
      val response = Json.toJson(request.handle).as[CreateLensResponse]
      val df = MimirAPI.catalog.get(response.name)
      val row = df.take(1)(0)
      row(row.fieldIndex("A")).asInstanceOf[AnyRef] must not(beAnInstanceOf[String])

    }
  }

}