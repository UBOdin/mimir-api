package org.mimirdb.spark

import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification

import org.mimirdb.api.{ SharedSparkTestInstance, MimirAPI }
import org.mimirdb.api.FormattedError


class SparkParserInjectionSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  def beforeAll: Unit = SharedSparkTestInstance.initAPI

  val inject = InjectedSparkSQL(spark)

  "Inject names into the parser" >> {
    val df = inject(
      "SELECT * FROM INJECTED_TABLE", 
      Map("INJECTED_TABLE" -> { () => MimirAPI.catalog.get("TEST_R") })
    )

    df.count() must be equalTo(7l)
  }

  "Disallow non-mapped names if requested" >> {
    inject("SELECT * FROM TEST_R", allowMappedTablesOnly = true) must throwA[FormattedError]
  }

  "Access tables in nested queries" >> {
    val df = inject(
      """
        SELECT B FROM R WHERE R.A IN (SELECT A FROM S)
      """, 
      Map(
        "R" -> { () => MimirAPI.catalog.get("TEST_R") },
        "S" -> { () => MimirAPI.catalog.get("TEST_R") }
      )
    )

    df.count() must be equalTo(7l)
  }

}