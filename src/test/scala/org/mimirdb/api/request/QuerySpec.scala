package org.mimirdb.api.request

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.apache.spark.sql.functions._

import org.mimirdb.api.{ SharedSparkTestInstance, MimirAPI, Schema } 
import org.mimirdb.caveats.implicits._ 
import org.mimirdb.lenses.{ Lenses, LensConstructor }
import org.mimirdb.data.RangeConstructor
import play.api.libs.json.JsString


class QuerySpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll 
  {
    SharedSparkTestInstance.initAPI

    {
      val q = df.select( 
        $"id", 
        $"id" * 3 as "val",
        concat($"id".cast("string"), lit("_THNGIE")) as "str"
      )
      q.createOrReplaceTempView("QuerySpec")
    }
    {
      val q = df.select(
        $"id", 
        $"id".caveat("Hi!") as "badid"
      )
      q.createOrReplaceTempView("QuerySpecCaveat")
    }
  }

  def query[T](query: String, includeUncertainty: Boolean = true)
              (op: DataContainer => T): T = 
    op(Query(query, includeUncertainty, spark))

  def schemaOf(query: String): Seq[Schema] =
    Query.getSchema(query, spark)

  "Schema Lookups" >> {
    "lookup schemas" >> {
      schemaOf("SELECT * FROM QuerySpec") must beEqualTo(
        Seq(Schema("id", "long"), Schema("val", "long"), Schema("str", "string"))
      )
    }
  }

  "The Query Processor" >> {
    "perform simple queries without caveats" >> { 
      query("SELECT id FROM QuerySpec", false) { result => 
        // ID field must be untouched
        result.data.map { _(0) } must beEqualTo(Seq(0, 1, 2, 3, 4))

        // Shouldn't be any overlap in rowids
        result.prov.toSet.size must beEqualTo(5)
      }
    }

    "perform simple queries with caveats" >> {
      query("SELECT id FROM QuerySpec") { result => 
        // ID field must be untouched
        result.data.map { _(0) } must beEqualTo(Seq(0, 1, 2, 3, 4))

        // Shouldn't be any overlap in rowids
        result.prov.toSet.size must beEqualTo(5)

        // The one column should be untainted
        result.colTaint must beEqualTo(
          Seq(Seq(false), Seq(false), Seq(false), Seq(false), Seq(false))
        )

        // The rows should be untainted
        result.rowTaint must beEqualTo(
          Seq(false, false, false, false, false)
        )
      }

      query("SELECT id, badid FROM QuerySpecCaveat WHERE id >= 3 OR badid < 4") { result =>
        result.data.map { _(1) } must beEqualTo(Seq(0, 1, 2, 3, 4))

        // The one column should be untainted
        result.colTaint must beEqualTo(
          Seq(Seq(false, true), Seq(false, true), Seq(false, true), Seq(false, true), Seq(false, true))
        )

        // The rows should be untainted
        result.rowTaint must beEqualTo(
          Seq(true, true, true, false, false)
        )
      }
      
      query("SELECT COUNT(*) FROM QuerySpecCaveat") { result =>
        result.data.map { _(0) } must beEqualTo(Seq(5))
      }
    }
    "Query a catalog table" >> 
    {
      val table = "QUERY_R"
      SharedSparkTestInstance.loadCSV("QUERY_R", "test_data/r.csv")
      query("SELECT COUNT(*) FROM QUERY_R") { result =>
        result.data.map { _(0) } must beEqualTo(Seq(7))
      } 
    }
    "Query a lens in the catalog" >> 
    {
      val table = "SEQ"
      val output = "QUERY_MKL"
      val missingKey = Lenses("MISSING_KEY")
      val df = dataset(table)
      val config = missingKey.train(df, JsString("KEY"))
      MimirAPI.catalog.put(
        output,
        LensConstructor(
          "MISSING_KEY",
          table, 
          config, 
          "in " + table  
        ),
        Set(table)
      )
      query(s"SELECT * FROM $output LIMIT 10", true) { result =>
        result.data.map { _(0) }.toSeq must beEqualTo((1 until 11).toSeq)
      } 
    }
    "Not explode when querying oversized datasets" >>
    {
      val table = "BIG_SEQ"
      MimirAPI.catalog.put(
        table,
        RangeConstructor(0, Query.RESULT_THRESHOLD+10, 1),
        Set()
      )
      query(s"SELECT * FROM $table") { 
        result => ko
      } must throwA[ResultTooBig]
    }
    "Respond with sane ROWIDs" >> 
    {
      {
        val ret = Query("SELECT * FROM QuerySpec", false)
        // println(ret.prov)
        ret.prov.toSet.size must be equalTo(ret.data.size)
      }
      {
        val ret = Query("SELECT * FROM TEST_R", false)
        // println(ret.prov)
        ret.prov.toSet.size must be equalTo(ret.data.size)
      }

    }
  }

}