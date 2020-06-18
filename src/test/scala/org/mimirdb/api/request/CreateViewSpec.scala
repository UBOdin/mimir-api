package org.mimirdb.api.request

import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.spark.GetViewDependencies


class CreateViewSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  "Check that spark represents views in the expected way" >> 
  {
    val query = spark.sql("SELECT * FROM TEST_R, GEO")
    // query.explain(true)
    GetViewDependencies(query).map { _.toLowerCase() } must contain(
      "test_r", "geo"
    )
  }
}