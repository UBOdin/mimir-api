package org.mimirdb.api

import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.mimirdb.api.request._


class PropertiesSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll 
  {
    SharedSparkTestInstance.initAPI
  }

  "dataset properties" >> {
    LoadRequest(
      file              = "test_data/r.csv",
      format            = "csv",
      inferTypes        = true,
      detectHeaders     = true,
      humanReadableName = Some("R_WITH_PROPERTIES"),
      backendOption     = Seq.empty,
      dependencies      = None,
      resultName        = Some("R_WITH_PROPERTIES"),
      properties        = Some(Map(
        "shazbot" -> JsString("frobbed"),
        "dingbat" -> Json.obj("a" -> JsNumber(123), "b" -> JsNull)
      )),
      proposedSchema    = Seq()
    ).handle
    val result = 
      Json.toJson(QueryTableRequest("R_WITH_PROPERTIES", None, None, None, true)
        .handle)
        .as[DataContainer]

    result.properties("shazbot").as[String] must beEqualTo("frobbed")
    val dingbats = 
      result.properties("dingbat").as[Map[String, JsValue]]
    dingbats("a").as[Int] must beEqualTo(123)
    dingbats("b") must beEqualTo(JsNull)
  }
}