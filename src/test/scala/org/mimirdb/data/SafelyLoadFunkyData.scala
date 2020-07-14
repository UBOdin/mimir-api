package org.mimirdb.data

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.mimirdb.api.{ SharedSparkTestInstance, MimirAPI }

class SafelyLoadFunkyData
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  "Data with funky unicode" >> {
    val df = MimirAPI.catalog.put(
      "CURE_SOURCE",
      LoadConstructor(
        "test_data/cureSource.csv",
        "csv",
        Catalog.defaultLoadOptions("csv", true)
      ),
      Set.empty,
      true,
      Map.empty
    )

    df.count() must be equalTo(8910l)
  }
}