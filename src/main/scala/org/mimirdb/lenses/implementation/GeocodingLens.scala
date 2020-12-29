package org.mimirdb.lenses.implementation

import scala.util.Random
import play.api.libs.json._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ 
  udf, 
  size => array_size, 
  element_at, 
  when,
  col, 
  concat, 
  lit
}

import org.mimirdb.lenses.Lens
import org.mimirdb.data.Catalog
import org.mimirdb.util.HttpUtils
import org.mimirdb.caveats.implicits._
import org.mimirdb.util.StringUtils
import java.net.URLEncoder

case class GeocoderConfig(
  houseColumn: String,
  streetColumn: String,
  cityColumn: String,
  stateColumn: String,
  geocoder: Option[String],
  latitudeColumn: Option[String],
  longitudeColumn: Option[String],
  cacheCode: Option[String]
)
{
  def withGeocoderAndCacheCode(newGeocoder: String, newCacheCode: String) =
    GeocoderConfig(
      houseColumn,
      streetColumn,
      cityColumn,
      stateColumn,
      Some(newGeocoder),
      latitudeColumn,
      longitudeColumn,
      Some(newCacheCode)
    )

  def lat = latitudeColumn.getOrElse { "LATITUDE" }
  def lon = latitudeColumn.getOrElse { "LONGITUDE" }
}

object GeocoderConfig
{
  implicit val format: Format[GeocoderConfig] = Json.format
}


class GeocodingLens(
  geocoders:Map[String,Geocoder],
  catalog: Catalog,
  cacheFormat: String = "json"
)
  extends Lens
{

  val HOUSE = "HOUSE"
  val STREET = "STREET"
  val CITY = "CITY"
  val STATE = "STATE"
  val COORDS = "COORDS"

  def train(input: DataFrame, rawConfig:JsValue): JsValue = 
  {
    val config = rawConfig.as[GeocoderConfig]
    val geocoder = config.geocoder
                          .getOrElse { geocoders.head._1 }
    val cacheCode = config.cacheCode.getOrElse { 
      "GEOCODE_" + geocoder + "_" + (new Random().alphanumeric.take(20).mkString)
    }
    val geocodeFn = geocoders(geocoder).apply _
    val geocode = udf(geocodeFn)
    
    val addresses = input.select(
      input(config.houseColumn ) as HOUSE,
      input(config.streetColumn) as STREET,
      input(config.cityColumn  ) as CITY,
      input(config.stateColumn ) as STATE
    )

    val cachedAddresses = catalog.getOption(cacheCode)

    val someRequiredAddressesAreNotCachedYet = 
      (cachedAddresses.isEmpty) ||
        (! (addresses except (
          cachedAddresses.map { cache =>
            cache.select(
              cache(HOUSE) as HOUSE,
              cache(STREET) as STREET,
              cache(CITY) as CITY,
              cache(STATE) as STATE
            )
          }.get)).isEmpty)

    if(someRequiredAddressesAreNotCachedYet){
      // This can be optimized, but let's start simple.  If the cache is 
      // incomplete, throw it all away and start from scratch.
      val newCache = addresses.select( 
                          addresses(HOUSE),
                          addresses(STREET),
                          addresses(CITY),
                          addresses(STATE),
                          geocode(
                            addresses(HOUSE).cast(StringType),
                            addresses(STREET).cast(StringType),
                            addresses(CITY).cast(StringType),
                            addresses(STATE).cast(StringType)
                          ) as COORDS
                      )
      catalog.stageAndPut(cacheCode, newCache, cacheFormat, includeRowId = true)
    } 
    return Json.toJson(config.withGeocoderAndCacheCode(geocoder, cacheCode))
  }

  def create(input: DataFrame, jsConfig: JsValue, context: String): DataFrame =
  {
    val config = jsConfig.as[GeocoderConfig]
    val cache = catalog.get(config.cacheCode.get)
    val geocoder = config.geocoder
                          .getOrElse { geocoders.head._1 }
    val coordinates = col("_2").getField(COORDS)

    def caveatMessage(multiplicity: String) = 
    {
      concat(
        lit(s"Geocoder $geocoder provided ${multiplicity} coordinates for '"),
        col("_2").getField(HOUSE).cast(StringType) , lit(" "),
        col("_2").getField(STREET).cast(StringType), lit("; "),
        col("_2").getField(CITY).cast(StringType)  , lit(", "),
        col("_2").getField(STATE).cast(StringType) , lit("'"),
      )      
    }
    val caveatKey = Seq(
      col("_2").getField(HOUSE), 
      col("_2").getField(STREET), 
      col("_2").getField(CITY), 
      col("_2").getField(STATE)
    )

    val fieldNames = input.schema.fieldNames.toSet

    def buildLatOrLon(lat: Boolean) =
    {
      val (coord_idx, stringLatOrLon, colName) = (
        if(lat) { (1, "latitude",  config.lat) }
        else    { (2, "longitude", config.lon) }
      )

      when(array_size(coordinates) >= 2,
           element_at(coordinates, coord_idx)
              .caveatIf(
                message = caveatMessage("multiple"),
                family = config.cacheCode.get,
                condition = array_size(coordinates) > 2
              )(caveatKey:_*)
      ).otherwise(
        lit(null).caveat(
                message = caveatMessage("no"), 
                family = config.cacheCode.get
              )(caveatKey:_*)
      ).as(StringUtils.uniqueName(colName, fieldNames))
    }
    // cache.show()

    input.joinWith(
      cache,
      (
            (input(config.houseColumn) === cache(HOUSE))
        and (input(config.streetColumn) === cache(STREET))
        and (input(config.cityColumn) === cache(CITY))
        and (input(config.stateColumn) === cache(STATE))
      ),
      "left_outer"
    ).select(
      (
      input.schema
           .fieldNames
           .map { field => col("_1").getField(field).as(field) } 
        :+ buildLatOrLon(true) 
        :+ buildLatOrLon(false)
      ):_*

    )
  }

}



abstract class Geocoder(val name: String) extends Serializable {

  def apply(house: String, street: String, city: String, state: String): Seq[Double]

}

  
abstract class GeoValue(val value:Double) 

object GeoValue{
  implicit val format: Format[GeoValue] = Format(
  new Reads[GeoValue] {
    def reads(json: JsValue): JsResult[GeoValue] = {
      json match {
        case JsString(s) => JsSuccess(GeoString(s))
        case JsNumber(d) => JsSuccess(GeoDouble(d.toDouble))
        case x => throw new Exception(s"GeoValue: $x not supported")
      }
    }
  }, new Writes[GeoValue] { 
      def writes(data: GeoValue): JsValue = {
        data match {
          case GeoString(s) => JsString(s)
          case GeoDouble(d) => JsNumber(d)
          case x => throw new Exception(s"GeoValue: $x not supported")
        }
      }
  })
}
case class GeoString(s: String) extends GeoValue(s.toDouble)
case class GeoDouble(d: Double) extends GeoValue(d)


abstract class WebJsonGeocoder(
  getLat: JsPath, 
  getLon: JsPath,
  name: String
) 
  extends Geocoder(name)
  with LazyLogging
{

  def apply(house: String, street: String, city: String, state: String): Seq[Double]=
  {
    val actualUrl = url(
        Option(house).getOrElse(""), 
        Option(street).getOrElse(""), 
        Option(city).getOrElse(""), 
        Option(state).getOrElse(""))
    try {
      val json = Json.parse(HttpUtils.get(actualUrl))
      val latitude = getLat.read[GeoValue].reads(json).get.value
      val longitude = getLon.read[GeoValue].reads(json).get.value
      return Seq( latitude, longitude )
    } catch {
      case nse: java.util.NoSuchElementException => {
        if(Option(house).isEmpty && Option(street).isEmpty)
          return Seq()
        else
          return apply(null,null,city,state)
      }
      case ioe: Throwable =>  {
        logger.error(s"Exception with Geocoding Request: $actualUrl", ioe)
        Seq()
      }
    }
  }

  def url(house: String, street: String, city: String, state: String): String
}

class GoogleGeocoder(apiKey: String) extends WebJsonGeocoder(
  JsPath \ "results" \ 0 \ "geometry" \ "location" \ "lat",
  JsPath \ "results" \ 0 \ "geometry" \ "location" \ "lng",
  "GOOGLE"
)
{
  def url(house: String, street: String, city: String, state: String) =
    s"https://maps.googleapis.com/maps/api/geocode/json?address=${s"$house+${street.replaceAll(" ", "+")},+${city.replaceAll(" ", "+")},+$state".replaceAll("\\+\\+", "+")}&key=$apiKey"
}

class OSMGeocoder(hostURL: String, name: String = "OSM") extends WebJsonGeocoder(
  JsPath \ 0 \ "lat",
  JsPath \ 0 \ "lon",
  name
)
{
  def url(house: String, street: String, city: String, state: String) =
    s"$hostURL/?format=json&street=${URLEncoder.encode(house, "UTF-8")}%20${URLEncoder.encode(street, "UTF-8")}&city=${URLEncoder.encode(city, "UTF-8")}&state=${URLEncoder.encode(state, "UTF-8")}"
}

object TestCaseGeocoder extends Geocoder("TEST")
{
  def apply(house: String, street: String, city: String, state: String): Seq[Double] =
  {
    val rnd = new Random(Seq(house, street, city, state).mkString("; ").hashCode)

    val target = rnd.nextDouble

    if(target < 0.7){
      Seq(
        rnd.nextDouble * 180 - 90, 
        rnd.nextDouble * 360 - 180
      )
    } else if(target < 0.9){
      Seq(
        rnd.nextDouble * 180 - 90, 
        rnd.nextDouble * 360 - 180,
        rnd.nextDouble * 180 - 90, 
        rnd.nextDouble * 360 - 180
      )
    } else { Seq() }

  }
}