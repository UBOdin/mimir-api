package org.mimirdb.api.data

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField

import org.mimirdb.api.{ SharedSparkTestInstance, MimirAPI }
import org.mimirdb.caveats.implicits._ 
import org.mimirdb.lenses.LensConstructor
import org.mimirdb.lenses.Lenses
import play.api.libs.json.JsString
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator
import org.mimirdb.api.request.{ 
  CreateViewRequest,
  LoadResponse,
  LoadRequest,
  DataContainer,
  Query
}

class JoinSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll 
  {
    SharedSparkTestInstance.initAPI
    val request = LoadRequest(
                    file              = "test_data/covid.csv",
                    format            = "csv",
                    inferTypes        = true,
                    detectHeaders     = true,
                    humanReadableName = Some("covid_us_county"),
                    backendOption     = Seq(),
                    dependencies      = None,
                    resultName        = Some("covid_us_county"),
                    properties        = None
                  )
    val response = request.handle.as[LoadResponse]
  }

  def query[T](query: String, includeUncertainty: Boolean = true)
              (op: DataContainer => T): T = 
    op(Query(query, includeUncertainty, sparkSession = spark))

  def schemaOf(query: String): Seq[StructField] =
    Query.getSchema(query, spark)

  "JoinSpec" >> {
    "perform simple spacial queries with caveats" >> {
      CreateViewRequest(Map(("covid_us_county","covid_us_county")),
      s"""SELECT first(covid_us_county.COUNTY) AS COUNTY, 
          |       first(covid_us_county.STATE) AS STATE, 
          |       covid_us_county.FIPS, 
          |       last(covid_us_county.CASES) AS CASES, 
          |       last(covid_us_county.DEATHS) AS DEATHS 
          |    FROM covid_us_county
          |    WHERE FIPS IS NOT NULL AND cast(covid_us_county.DATE AS string) like '2020-03-%'
          |    GROUP BY FIPS""".stripMargin,
         Some("covid_us_county_03_eom"),
         None
      ).handle
      
      CreateViewRequest(Map(("covid_us_county","covid_us_county")),
      s"""SELECT first(covid_us_county.COUNTY) AS COUNTY, 
          |       first(covid_us_county.STATE) AS STATE, 
          |       covid_us_county.FIPS, 
          |       last(covid_us_county.CASES) AS CASES, 
          |       last(covid_us_county.DEATHS) AS DEATHS 
          |    FROM covid_us_county
          |    WHERE FIPS IS NOT NULL AND cast(covid_us_county.DATE AS string) like '2020-04-%'
          |    GROUP BY FIPS""".stripMargin,
         Some("covid_us_county_04_eom"),
         None
      ).handle
      
      CreateViewRequest(Map(("covid_us_county_03_eom","covid_us_county_03_eom"),("covid_us_county_04_eom","covid_us_county_04_eom")),
          s"""SELECT covid_us_county_04_eom.COUNTY,
              |       covid_us_county_04_eom.STATE,
              |       covid_us_county_04_eom.FIPS,
              |       (covid_us_county_04_eom.CASES - covid_us_county_03_eom.CASES) AS CASES,
              |       (covid_us_county_04_eom.DEATHS - covid_us_county_03_eom.DEATHS) AS DEATHS
              |  FROM covid_us_county_04_eom
              |  LEFT JOIN covid_us_county_03_eom
              |    ON covid_us_county_03_eom.FIPS = covid_us_county_04_eom.FIPS""".stripMargin,
         Some("covid_us_county_03_only"),
         None
      ).handle 

      query("SELECT * FROM covid_us_county_03_only"){ result => 
        result.data.map { _(0) } 
        ok
      }
    }
  }

}