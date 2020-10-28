package org.mimirdb.api.data

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import play.api.libs.json._

import org.mimirdb.api.{ SharedSparkTestInstance, MimirAPI }
import org.mimirdb.caveats.implicits._ 
import org.mimirdb.lenses.LensConstructor
import org.mimirdb.lenses.Lenses
import play.api.libs.json.JsString
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparkviz.sql.utils.GeoSparkVizRegistrator
import org.mimirdb.api.request.{ 
  CreateViewRequest,
  LoadRequest,
  DataContainer,
  Query
}
import org.mimirdb.api.CreateResponse
import org.mimirdb.api.request.ExplainCellRequest

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
                    properties        = None,
                    proposedSchema    = None
                  )
    val response = Json.toJson(request.handle).as[CreateResponse]
  }

  def query[T](query: String, includeUncertainty: Boolean = true)
              (op: DataContainer => T): T = 
    op(Query(query, includeUncertainty, sparkSession = spark))

  def schemaOf(query: String): Seq[StructField] =
    Query.getSchema(query, spark)

  "JoinSpec" >> {
    "perform simple spacial queries with caveats" >> {
      CreateViewRequest(Map(("covid_us_county","covid_us_county")),None,
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
      
      CreateViewRequest(Map(("covid_us_county","covid_us_county")),None,
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
      
      CreateViewRequest(Map(("covid_us_county_03_eom","covid_us_county_03_eom"),("covid_us_county_04_eom","covid_us_county_04_eom")),None,
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
    
    "list caveats for simple left join" >> { 
      CreateViewRequest(Map(("TEST_R", "TEST_R")),None,
          """SELECT TEST_R.A AS A, TEST_R.B AS B, caveat(TEST_R.C, 'test') AS C FROM TEST_R""",
         Some("TEST_R"),
         None
      ).handle 
      
      CreateViewRequest(Map(("SEQ", "SEQ")),None,
          """SELECT * FROM SEQ""",
         Some("SEQ"),
         None
      ).handle 
      
      CreateViewRequest(Map(("TEST_R", "TEST_R"),("SEQ", "SEQ")),None,
          s"""SELECT TEST_R.A,
              |      TEST_R.C,
              |      SEQ.NAME
              |  FROM TEST_R
              |  LEFT JOIN SEQ
              |    ON TEST_R.A = SEQ.KEY""".stripMargin,
         Some("TEST_R_SEQ"),
         None
      ).handle 
      
      val result = Query("SELECT * FROM TEST_R_SEQ",true).prov
      ExplainCellRequest(
        query = "SELECT * FROM TEST_R_SEQ",
        row = result.head,
        col = "C"   
      ).handle 
      ok
    }
  }

}