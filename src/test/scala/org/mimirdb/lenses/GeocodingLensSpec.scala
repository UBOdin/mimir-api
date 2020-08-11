package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.lenses.implementation.TestCaseGeocoder


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
      }

    }

    ok

  }
}
