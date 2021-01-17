package org.mimirdb.api.request

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import play.api.libs.json._
import org.mimirdb.api.{ SharedSparkTestInstance, MimirAPI } 
import org.mimirdb.caveats.implicits._ 
import org.mimirdb.lenses.{ Lenses, LensConstructor }
import org.mimirdb.data.RangeConstructor
import org.mimirdb.profiler.DataProfiler
import play.api.libs.json.{ JsString, JsNull }
import org.mimirdb.spark.{ SparkPrimitive, Schema }


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
        concat($"id".cast("string"), lit("_THNGIE")) as "str",
        when($"id" === 1, concat($"id".cast("string"), lit("_FAIL")))
          .otherwise($"id".cast("string")) as "somestr"
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
    op(Query(query, includeUncertainty, sparkSession = spark))

  def schemaOf(query: String): Seq[StructField] =
    Query.getSchema(query, spark)

  def queryTable[T](
    table: String,
    columns: Seq[String] = null,
    limit: Integer = null,
    offset: Integer = null,
    includeUncertainty: Boolean = false,
    profile: Boolean = false
  )(op: DataContainer => T): T = 
    op( Json.toJson(QueryTableRequest(
          table,
          Option(columns),
          Option(limit).map { _.toInt },
          Option(offset).map { _.toLong },
          includeUncertainty,
          Some(profile)
        ).handle).as[DataContainer] )

  "Basic Query Functions" >> {
    "Look up schemas" >> {
      schemaOf("SELECT * FROM QuerySpec") must beEqualTo(
        Seq(
          StructField("id", Schema.decodeType("long"), false), 
          StructField("val", Schema.decodeType("long"), false), 
          StructField("str", Schema.decodeType("string"), false), 
          StructField("somestr", Schema.decodeType("string"), false)
        )
      )
    }

    "Perform simple queries without caveats" >> { 
      query("SELECT id FROM QuerySpec", false) { result => 
        // ID field must be untouched
        result.data.map { _(0) } must beEqualTo(Seq(0, 1, 2, 3, 4))

        // Shouldn't be any overlap in rowids
        result.prov.toSet.size must beEqualTo(5)
      }
    }

    "Perform simple queries with caveats" >> {
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

    "Perform Left Join with caveats" >> {
      query("""
        SELECT DISTINCT a.B FROM TEST_R a
          LEFT JOIN TEST_R b ON b.A = a.A
        WHERE b.B IS NULL
      """) { result => 
        result.data.size must beEqualTo(2)
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
      val missingKey = Lenses(Lenses.missingKey)
      val df = dataset(table)
      val config = missingKey.train(df, JsString("KEY"))
      MimirAPI.catalog.put(
        output,
        LensConstructor(
          Lenses.missingKey,
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
  }

  "Query Regressions" >> {
    "Don't explode when querying oversized datasets" >>
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

    "Apply caveats to CAST" >> 
    {
      query("SELECT CAST(somestr as int) FROM QuerySpec") { result => 
        result.data.map { _(0) } must beEqualTo(Seq(0, null, 2, 3, 4))
        result.colTaint.map { _(0) } must beEqualTo(Seq(false, true, false, false, false))
      }
    }

    "Not apply CAST caveats to source tables" >>
    {
      df.select(
        when($"id" === 1, concat($"id".cast("string"), lit("_FAIL")))
          .otherwise($"id".cast("string"))
          .cast("int") as "somestr"
      ).createOrReplaceTempView("HIDDEN_CAST_TEST")

      query("SELECT somestr FROM HIDDEN_CAST_TEST") { result => 
        result.data.map { _(0) } must beEqualTo(Seq(0, null, 2, 3, 4))
        result.colTaint.map { _(0) } must beEqualTo(Seq(false, false, false, false, false))
      }

    }
  }

  "Table Queries" >> {
    "Query Tables" >>
    {
      queryTable("TEST_R") { result =>
        result.data must haveSize(7)
      }
    }

    "Query Tables with Offset/Limit" >> 
    {
      val data = queryTable("TEST_R") { result => result.data }

      queryTable("TEST_R", limit = 2) { result => 
        result.schema.map { _.name } must contain(exactly("A", "B", "C"))
        result.data must beEqualTo(data.take(2))
      }

      queryTable("TEST_R", offset = 2, limit = 2) { result =>
        result.schema.map { _.name } must contain(exactly("A", "B", "C"))
        result.data must beEqualTo(data.drop(2).take(2))
      }
    }

    "Get Table Sizes" >> { 
      SizeOfTableRequest("TEST_R").handle.size must beEqualTo(7)
    }

    "Profile if needed (and preserve properties)" >> {
      // If we ask for the data to be profiled, we'd better get a
      // profile back
      queryTable("TEST_R", profile = true) { result => 
        result.properties.keys must contain(DataProfiler.IS_PROFILED)
      }

      // Once the data is profiled, we'd better keep getting the
      // same profile back
      queryTable("TEST_R", profile = false) { result => 
        result.properties.keys must contain(DataProfiler.IS_PROFILED)
      }
    }
  }

  "Query possible answers" >> {
    query("""
      SELECT * FROM (
        SELECT CAVEATIF(A, A IS NULL, "FOO") AS A,
               CAVEATIF(B, B IS NULL, "FOO") AS B,
               CAVEATIF(C, C IS NULL, "FOO") AS C
        FROM TEST_R
      ) TEST_R 
      WHERE POSSIBLE(B = 2) AND C = 1
    """) { result => 
      result.data.size must beEqualTo(2)
    }
  }
}