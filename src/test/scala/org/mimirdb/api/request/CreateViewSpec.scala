package org.mimirdb.api.request

import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.spark.InjectedSparkSQL


class CreateViewSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  "Check that spark represents views in the expected way" >> 
  {
    val query = InjectedSparkSQL(spark)("SELECT * FROM TEST_R, GEO", MimirAPI.catalog.allTableConstructors)
    // query.explain(true)
    val (viewDeps, fnDeps) = 
      InjectedSparkSQL(spark).getDependencies("SELECT * FROM TEST_R, GEO")
    
    viewDeps must contain(
      "test_r", "geo"
    )
  }

  "Check that we are able to extract aliased views" >> 
  {
    val views = 
      InjectedSparkSQL(spark)
        .getDependencies("SELECT * FROM TEST_R foo")
        ._1

    views must contain(exactly("test_r"))
  }
}