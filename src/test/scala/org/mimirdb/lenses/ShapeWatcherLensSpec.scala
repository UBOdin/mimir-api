package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification
import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.profiler.shape.Facet
import org.mimirdb.api.request.CreateLensRequest
import org.mimirdb.caveats.implicits._

class ShapeWatcherLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll = {
    SharedSparkTestInstance.initAPI
    SharedSparkTestInstance.loadCSV("Z", "test_data/z.csv")
    SharedSparkTestInstance.loadCSV("Z_BAD", "test_data/z_bad.csv")
  }

    "Detect the basics" >> {
      val ret = Facet.detect(MimirAPI.catalog.get("Z")).map { _.description }
      ret must contain { contain("A, B") }.atLeastOnce // columns
      ret must contain { contain("A should be a string") }.atLeastOnce // columns
    }
    
    "Detect facets and test them" >> {
      //good data
      val goodLens = CreateLensRequest(
        input = "Z",
        params = JsNull,
        `type` = "shape_watcher",
        resultName = Some("Z_SW"),
        materialize = false,
        humanReadableName = Some("Z Data"),
        properties = None
      ).handle

      val badLens = CreateLensRequest(
        input = "Z_BAD",
        params = goodLens.config,
        `type` = "shape_watcher",
        resultName = Some("Z_BAD_SW"),
        materialize = false,
        humanReadableName = Some("Z Data (bad)"),
        properties = None
      ).handle
      
      val caveats = 
          MimirAPI.catalog.get(badLens.name)
                  .listCaveats()
      
      caveats.map { _.message } must contain(eachOf(
        "Missing expected column 'B'",
        "A had no nulls before, but now has 1",
        "Unexpected column 'B_0'"
      ))
    }

 }
