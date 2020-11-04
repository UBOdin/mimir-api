package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.lenses.implementation.TestCaseGeocoder
import org.mimirdb.api.request.LoadRequest
import org.mimirdb.api.request.CreateLensRequest
import org.mimirdb.api.CreateResponse
import org.mimirdb.api.request.CreateLensResponse
import org.mimirdb.api.request.QueryTableRequest
import org.mimirdb.api.request.DataContainer


class GeocodingLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  val STRNUMBER = "STRNUMBER"
  val STRNAME = "STRNAME"
  val CITY = "CITY"
  val STATE = "STATE"
  val LATITUDE = "LATITUDE"
  val LONGITUDE = "LONGITUDE"

  def beforeAll = SharedSparkTestInstance.initAPI

  "Geocoding Lens" >> {

    val geocoder = Lenses(Lenses.geocode)
    val df = MimirAPI.catalog.get("GEO")
    val config = geocoder.train(df, Json.obj(
      "houseColumn"  -> STRNUMBER,
      "streetColumn" -> STRNAME,
      "cityColumn"   -> CITY,
      "stateColumn"  -> STATE      
    ))
    val geocoded = geocoder.create(df, config, "TEST_CASE")

    geocoded.schema.fieldNames.toSet must contain(eachOf(LATITUDE, LONGITUDE))

    for(row <- geocoded.collect()){
      val geo = TestCaseGeocoder(
        row.getAs[String](STRNUMBER),
        row.getAs[String](STRNAME),
        row.getAs[String](CITY),
        row.getAs[String](STATE)
      )

      geo match {
        case Seq() => {
          row.isNullAt(row.fieldIndex(LATITUDE)) must beTrue
          row.isNullAt(row.fieldIndex(LONGITUDE)) must beTrue
        }
        case Seq(a, b) => {
          row.getAs[Double](LATITUDE) must beEqualTo(a)
          row.getAs[Double](LONGITUDE) must beEqualTo(b)
        }
        case Seq(a, b, c, d) => {
          row.getAs[Double](LATITUDE) must beEqualTo(a)
          row.getAs[Double](LONGITUDE) must beEqualTo(b)
        }
      }

    }

    val loadRequest:LoadRequest = LoadRequest(
                    file              = "test_data/geo.csv",
                    format            = "csv",
                    inferTypes        = true, 
                    detectHeaders     = true,
                    humanReadableName = Some("STILL MORE THING TESTS"),
                    backendOption     = Seq(),
                    dependencies      = Some(Seq()),
                    resultName        = None,
                    properties        = None,
                    proposedSchema    = None
                  )            
    val loadResponse = Json.toJson(loadRequest.handle).as[CreateResponse]
      
    val lensRequest = CreateLensRequest(
                      loadResponse.name,
                      Json.obj(
                        "houseColumn"  -> STRNUMBER,
                        "streetColumn" -> STRNAME,
                        "cityColumn"   -> CITY,
                        "stateColumn"  -> STATE      
                      ), 
                      Lenses.geocode,
                      false,
                      Some("NULL TEST"),
                      None,
                      None
                    )
    val LensResponse = Json.toJson(lensRequest.handle).as[CreateLensResponse]
     
    val queryRequest =  QueryTableRequest(
                            LensResponse.name,
                            None,
                            Some(25),
                            Some(0),
                            true,
                            Some(true))
    val queryResponse = Json.toJson(queryRequest.handle).as[DataContainer]
    
    ok

  }
}
