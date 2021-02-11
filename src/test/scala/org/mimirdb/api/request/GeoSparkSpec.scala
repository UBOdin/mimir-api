package org.mimirdb.api.request

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
import org.mimirdb.api.CreateResponse

class GeoSparkSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll 
  {
    SharedSparkTestInstance.initAPI
    //Initialize GeoSpark
    System.setProperty("sedona.global.charset", "utf8")
    
    
    val request = LoadRequest(
                    file              = "test_data/social_dist.csv",
                    format            = "csv",
                    inferTypes        = true,
                    detectHeaders     = true,
                    humanReadableName = Some("social_dist"),
                    backendOption     = Seq(),
                    dependencies      = Some(Seq()),
                    resultName        = Some("social_dist"),
                    properties        = Some(Map.empty),
                    proposedSchema    = None
                  )
    val response = Json.toJson(request.handle).as[CreateResponse]
    
    val request2 = LoadRequest(
                    file              = "test_data/census_geo.csv",
                    format            = "csv",
                    inferTypes        = true,
                    detectHeaders     = true,
                    humanReadableName = Some("census_geo"),
                    backendOption     = Seq(),
                    dependencies      = Some(Seq()),
                    resultName        = Some("census_geo"),
                    properties        = Some(Map.empty),
                    proposedSchema    = None
                  )
    val response2 = Json.toJson(request2.handle).as[CreateResponse]
    
      
  }

  def query[T](query: String, includeUncertainty: Boolean = true)
              (op: DataContainer => T): T = 
    op(Query(query, includeUncertainty, sparkSession = spark))

  def schemaOf(query: String): Seq[StructField] =
    Query.getSchema(query, spark)

  "Geospark" >> {
    "perform simple spacial queries with caveats" >> {
      CreateViewRequest(Map(("social_dist","social_dist"),("census_geo","census_geo")),None,
      s"""SELECT social_dist.*, census_geo.LATITUDE, census_geo.LONGITUDE,
         |  ST_Point(CAST(census_geo.LONGITUDE AS Decimal(24,20)),
         |       CAST(census_geo.LATITUDE AS Decimal(24,20))) AS PT_SHAPE
         |FROM social_dist
         |LEFT JOIN census_geo
         |on census_geo.CENSUS_BLOCK_GROUP = social_dist.ORIGIN_CENSUS_BLOCK_GROUP""".stripMargin,
         Some("social_dist_geo"),
         None
      ).handle
      
      CreateViewRequest(Map(("social_dist_geo","social_dist_geo")),None,
          "SELECT ST_Envelope_Aggr(social_dist_geo.PT_SHAPE) AS BOUND FROM social_dist_geo",
          Some("social_dist_bound"),
         None
      ).handle 
      
      CreateViewRequest(Map(("social_dist_bound","social_dist_bound")),None,
          s"""SELECT ST_Transform(BOUND, 'epsg:4326','epsg:3857') AS TRANS_BOUND
          FROM social_dist_bound""",
         Some("social_dist_bound_trans"),
         None
      ).handle 

      
      CreateViewRequest(Map(("social_dist_geo","social_dist_geo"), ("social_dist_bound","social_dist_bound"), ("social_dist_bound_trans","social_dist_bound_trans")),None,
      s"""SELECT ST_Pixelize(ST_Transform(PT_SHAPE, 'epsg:4326','epsg:3857'), 256, 256, 
          social_dist_bound_trans.TRANS_BOUND) AS PIXEL, 
          social_dist_geo.PT_SHAPE 
          FROM social_dist_geo
          LEFT JOIN social_dist_bound_trans
        """,
         Some("social_dist_pixel"),
         None
      ).handle 
      
      query("SELECT * FROM social_dist_geo"){ result => 
        result.data.map { _(0) } 
        ok
      }
      query("SELECT * FROM social_dist_bound"){ result => 
        result.data.map { _(0) } 
        ok
      }
      query("SELECT * FROM social_dist_pixel"){ result => 
        result.data.map { _(0) } 
        ok
      }
    }
  }

}