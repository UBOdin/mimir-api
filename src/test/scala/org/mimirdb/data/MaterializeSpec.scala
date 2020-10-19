package org.mimirdb.data

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.mimirdb.api.{ SharedSparkTestInstance, MimirAPI }

class MaterializeSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  "Materialize Tables" >> {
    MimirAPI.catalog.put(
      "MATERIALIZE_test",
      MaterializeConstructor("TEST_R", MimirAPI.catalog),
      Set("TEST_R")
    )

    val r    = MimirAPI.catalog.get("TEST_R")
                       .collect()
                       .map { _.toSeq }
                       .toSeq
    val mat  = MimirAPI.catalog.get("MATERIALIZE_test")
                       .collect()
                       .map { _.toSeq }
                       .toSeq
    val prov = MimirAPI.catalog.getProvenance("MATERIALIZE_test")
                       .collect()
                       .map { _.toSeq }
                       .toSeq

    mat must containTheSameElementsAs(r)
    prov must containTheSameElementsAs(r)
  }

}
