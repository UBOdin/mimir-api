package org.mimirdb.data

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.mimirdb.api.{ SharedSparkTestInstance, MimirAPI }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.implicits._

class SafelyLoadFunkyData
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  // "Data with funky unicode - text edition" >> {
  //   val df = MimirAPI.catalog.put(
  //     "CURE_SOURCE_TEXT",
  //     LoadConstructor(
  //       "test_data/cureSource.csv",
  //       "text",
  //       Catalog.defaultLoadOptions("csv", true)
  //     ),
  //     Set.empty,
  //     true,
  //     Map.empty
  //   )

  //   // df.count() must be equalTo(8910l)
  //   val ret = 
  //     df.collect()
  //       .map { _.getString(0) }
  //   ret.size must beEqualTo(8911)
  //   ret.toSeq must not(contain(beNull))

  //   val fields = Seq("Date","Month","Consignee Declared","Consignee Declared Address","Shipper Declared","Shipper Address","Notify Name","Notify Address","Carrier Code","Carrier","Bill Master Carrier","Master Consignee (Unified)","Master Shipper","Master Notify","Consignee (Unified)","Shipper (Unified)","Consignee's State","Consignee's City","Consignee's Zip Code","Bill of lading Nbr.","Master/House","Mode of Transport","Estimated Date","In bond entry type","Short Container Description","IMO Code","High Cube","Bill Master","HS","HS Description","Foreign Destination","US Region","World Region by Port of Departure","Country by Port of Departure","Port of Departure","State of Arrival Port","Port of Arrival","Vessel","Vessel Country","Final Destination","Country of Origin","World Region by Country of Origin","Place of Receipt","Country by Place of Receipt","World Region by Place of Receipt","Quantity","Quantity Unit","Weight","Weight Unit","Measure","Measure Unit","Container Quantity","Metric Tons","Container","Pieces","Description","Harmonized","Marks & Numbers","HS","Container","Pieces","Description","Harmonized","Marks & Numbers","HS","Container","Pieces","Description","Harmonized","Marks & Numbers","HS","Container","Pieces","Description","Harmonized","Marks & Numbers","HS","Container","Pieces","Description","Harmonized","Marks & Numbers")
  //                 .map { StructField(_, StringType )}

  //   df.select( from_csv(col("value"), StructType(fields), Map[String,String]()) as "csv_data" )
  //     .select( "csv_data.`Marks & Numbers`")
  //     .collect()
  //     .size must beEqualTo(8911)
  // }

  "Data with funky unicode - csv edition" >> {
    val df = MimirAPI.catalog.put(
      "CURE_SOURCE_CSV",
      LoadConstructor(
        "test_data/cureSource.csv",
        "csv",
        Catalog.defaultLoadOptions("csv", true)
      ),
      Set.empty,
      true,
      Map.empty
    )

    df.stripCaveats.explain()
    // df.count() must be equalTo(8910l)
    val ret = 
      df.select(col("date"))
        .collect()
        .map { _.getString(0) }
    ret.size must beEqualTo(8911)
    ret.toSeq must not(contain(beNull))
  }

}