package org.mimirdb.api.request

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.apache.spark.sql.functions._

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.Schema

import org.mimirdb.caveats.implicits._ 


class QuerySpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll 
  {
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

  }

}