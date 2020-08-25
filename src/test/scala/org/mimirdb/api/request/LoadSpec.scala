package org.mimirdb.api.request

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import play.api.libs.json._
import org.apache.spark.sql.types._

import org.mimirdb.api.MimirAPI
import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.CreateResponse
import org.mimirdb.caveats.implicits._
import org.mimirdb.api.Tuple

class LoadSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll = SharedSparkTestInstance.initAPI

  "load local files" >> {
    val request = LoadRequest(
                    file              = "test_data/r.csv",
                    format            = "csv",
                    inferTypes        = false,
                    detectHeaders     = true,
                    humanReadableName = Some("A TEST OF THE THING"),
                    backendOption     = Seq(),
                    dependencies      = Some(Seq()),
                    resultName        = None,
                    properties        = None,
                    proposedSchema    = None
                  )
    val response = Json.toJson(request.handle).as[CreateResponse]

    MimirAPI.catalog.get(response.name)
                    .count() must be equalTo(7)

    MimirAPI.catalog.flush(response.name)

    MimirAPI.catalog.get(response.name)
                    .count() must be equalTo(7)

  }

  "load remote files" >> {
    val request = LoadRequest(
                    file              = "https://odin.cse.buffalo.edu/public_data/r.csv",
                    format            = "csv",
                    inferTypes        = false,
                    detectHeaders     = true,
                    humanReadableName = Some("ANOTHER TEST OF THE THING"),
                    backendOption     = Seq(),
                    dependencies      = Some(Seq()),
                    resultName        = None,
                    properties        = None,
                    proposedSchema    = None
                  )
    val response = Json.toJson(request.handle).as[CreateResponse]

    MimirAPI.catalog.get(response.name)
                    .count() must be equalTo(7)

    MimirAPI.catalog.flush(response.name)

    MimirAPI.catalog.get(response.name)
                    .count() must be equalTo(7)
  }

  "load files with type inference" >> {

    val request = LoadRequest(
                    file              = "test_data/r.csv",
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
    val response = Json.toJson(request.handle).as[CreateResponse]

    val allRows = 
      MimirAPI.catalog.get(response.name).collect()
    val dataRow = 
      allRows(0)
    dataRow.schema.fieldNames.toSet must contain("A")
    dataRow.fieldIndex("A") must be greaterThanOrEqualTo(0)
    val firstCell = dataRow.getAs[AnyRef]("A")
    firstCell must beAnInstanceOf[java.lang.Short]
  }

  "load inlined datasets" >> {

    val request = LoadInlineRequest(
                    schema = Seq(
                      StructField("num", IntegerType),
                      StructField("str", StringType)
                    ),
                    data = Seq(
                      Seq[JsValue](JsNumber(1), JsString("A")),
                      Seq[JsValue](JsNumber(2), JsString("B")),
                      Seq[JsValue](JsNumber(3), JsString("C"))
                    ),
                    dependencies = None,
                    resultName = Some("INLINED_LOAD_TEST"),
                    properties = None,
                    humanReadableName = None
                  )

    val response = Json.toJson(Json.toJson(request).as[LoadInlineRequest].handle).as[CreateResponse]
    response.name must beEqualTo("INLINED_LOAD_TEST")

    val allRows = 
      MimirAPI.catalog.get(response.name).collect()
    val dataRow =
      allRows(0)
    dataRow.schema.fieldNames.toSet must contain(eachOf("num", "str"))
    dataRow.getAs[AnyRef]("num") must beEqualTo(1)
    dataRow.getAs[AnyRef]("str") must beEqualTo("A")
  }

  "load broken CSV files" >> {

    val request = LoadRequest(
                    file              = "test_data/pd5h-92mc.csv",
                    format            = "csv",
                    inferTypes        = true, 
                    detectHeaders     = true,
                    humanReadableName = Some("Garbled CSV"),
                    backendOption     = Seq(),
                    dependencies      = Some(Seq()),
                    resultName        = None,
                    properties        = None,
                    proposedSchema    = None
                  )
    val response = Json.toJson(request.handle).as[CreateResponse]

    val df = 
      MimirAPI.catalog.get(response.name)
    df.count() must beEqualTo(63l)
    df.collect().size must beEqualTo(63)

    df.listCaveats() must haveSize(21)

  }

  "load CSV files with unquoted headers" >> {

    val request = LoadRequest(
                    file              = "test_data/CPUSpeed.csv",
                    format            = "csv",
                    inferTypes        = true, 
                    detectHeaders     = true,
                    humanReadableName = Some("CPUSpeed-Unquoted"),
                    backendOption     = Seq(),
                    dependencies      = Some(Seq()),
                    resultName        = None,
                    properties        = None,
                    proposedSchema    = None
                  )
    val response = Json.toJson(request.handle).as[CreateResponse]

    ok

  }

  "override schemas" >> {

    val request = LoadRequest(
                    file              = "test_data/r.csv",
                    format            = "csv",
                    inferTypes        = false, 
                    detectHeaders     = true,
                    humanReadableName = Some("PROPOSED_SCHEMA_TEST"),
                    backendOption     = Seq(),
                    dependencies      = Some(Seq()),
                    resultName        = None,
                    properties        = None,
                    proposedSchema    = Some(Seq(
                                          StructField("ALICE", IntegerType),
                                          StructField("BOB", StringType)
                                        ))
                  )
    val response = Json.toJson(request.handle).as[CreateResponse]

    val df = MimirAPI.catalog.get(response.name)
    df.schema must haveSize(3)
    df.schema.fields(0) must beEqualTo(StructField("ALICE", IntegerType))
    df.schema.fields(1) must beEqualTo(StructField("BOB", StringType))
    df.schema.fields(2) must beEqualTo(StructField("C", StringType))
  }
  
  /*"load files from s3" >> {
    val request = LoadRequest(
                    file              = "s3a://mimir-test-data/test/data/mv.csv",
                    format            = "csv",
                    inferTypes        = false,
                    detectHeaders     = true,
                    humanReadableName = Some("S3 TEST OF THE THING"),
                    backendOption     = Seq(),
                    dependencies      = Some(Seq()),
                    resultName        = None,
                    properties        = None,
                    proposedSchema    = None
                  )
    val response = Json.toJson(request.handle).as[CreateResponse]

    MimirAPI.catalog.get(response.name)
                    .count() must be equalTo(8)

    MimirAPI.catalog.flush(response.name)

    MimirAPI.catalog.get(response.name)
                    .count() must be equalTo(8)
  }
  
  "load excel files" >> {
    val request = LoadRequest(
                    file              = "test_data/excel.xlsx",
                    format            = "com.crealytics.spark.excel",
                    inferTypes        = false,
                    detectHeaders     = true,
                    humanReadableName = Some("EXCEL OF THE THING"),
                    backendOption     = Seq(Tuple("sheetName", "SalesOrders"), // Required
                            Tuple("dataAddress", "'SalesOrders'!A1"),
                            Tuple("header", "false"), // Required
                            Tuple("treatEmptyValuesAsNulls", "true"), // Optional, default: true
                            Tuple("startColumn", "0"), // Optional, default: 0
                            Tuple("endColumn", "6") // Optional, default: Int.MaxValue
                            ),
                    dependencies      = Some(Seq()),
                    resultName        = None,
                    properties        = None,
                    proposedSchema    = None
                  )
    val response = Json.toJson(request.handle).as[CreateResponse]

    MimirAPI.catalog.get(response.name)
                    .count() must be equalTo(44)

    MimirAPI.catalog.flush(response.name)

    MimirAPI.catalog.get(response.name)
                    .count() must be equalTo(44)

  }*/
}