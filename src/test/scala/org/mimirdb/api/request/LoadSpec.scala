package org.mimirdb.api.request

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import org.mimirdb.api.MimirAPI
import org.mimirdb.api.SharedSparkTestInstance

class LoadSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll = SharedSparkTestInstance.initAPI

  "API-Load" >> {

    "load local files" >> {
      val request = LoadRequest(
                      "test_data/r.csv",
                      "csv",
                      false,
                      true,
                      Some("A TEST OF THE THING"),
                      Seq(),
                      Seq(),
                      None
                    )
      val response = request.handle.as[LoadResponse]

      MimirAPI.catalog.get(response.name)
                      .count() must be equalTo(7)

      MimirAPI.catalog.flush(response.name)

      MimirAPI.catalog.get(response.name)
                      .count() must be equalTo(7)

    }

    "load remote files" >> {
      val request = LoadRequest(
                      "https://odin.cse.buffalo.edu/public_data/r.csv",
                      "csv",
                      false,
                      true,
                      Some("ANOTHER TEST OF THE THING"),
                      Seq(),
                      Seq(),
                      None
                    )
      val response = request.handle.as[LoadResponse]

      MimirAPI.catalog.get(response.name)
                      .count() must be equalTo(7)

      MimirAPI.catalog.flush(response.name)

      MimirAPI.catalog.get(response.name)
                      .count() must be equalTo(7)
    }

    "load files with type inference" >> {

      val request = LoadRequest(
                      "test_data/r.csv",
                      "csv",
                      true, 
                      true,
                      Some("STILL MORE THING TESTS"),
                      Seq(),
                      Seq(),
                      None
                    )
      val response = request.handle.as[LoadResponse]

      val dataRow = 
        MimirAPI.catalog.get(response.name)
                        .take(1)(0)
      val firstCell = dataRow.get(dataRow.fieldIndex("A")).asInstanceOf[AnyRef]
      firstCell must beAnInstanceOf[java.lang.Short]
    }

  }
}