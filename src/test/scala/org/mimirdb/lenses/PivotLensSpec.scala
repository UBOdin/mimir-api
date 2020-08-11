package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification

import org.apache.spark.sql.DataFrame

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.lenses.implementation.PivotLensConfig
import org.mimirdb.caveats.{ Constants => Caveats }
import org.mimirdb.caveats.implicits._
import org.apache.spark.sql.Row

class PivotLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  import spark.implicits._

  def beforeAll = SharedSparkTestInstance.initAPI

  lazy val pivot = Lenses(Lenses.pivot)

  def test[T](
    target: String = "A",
    keys: Seq[String] = Seq(),
    values: Seq[String] = Seq("B"),
    input: DataFrame = MimirAPI.catalog.get("TEST_R")
  )(op: DataFrame => T):T = {
    val config = pivot.train(input, Json.toJson(PivotLensConfig(
                                          target = target, 
                                          keys = keys,
                                          values = values, 
                                          None
                                        )))
    op(pivot.apply(input, config, "TEST"))
  }

  "Pivot A Table To One Row" >> {
    test() { df => 
      df.columns.toSeq must contain(exactly("B_1", "B_2", "B_4"))
      val result:Seq[Row] = df.stripCaveats.collect().toSeq
      result must haveSize(1)
      val row:Row = result.head
      row.getString(0 /* B_1 */) must beOneOf("2", "3", "4")
      row.getString(1 /* B_2 */) must beEqualTo("2") // the only other option is NULL
      row.getString(2 /* B_4 */) must beEqualTo("2")
    }
  }
  "Pivot A Table To Multiple Rows" >> {
    test(keys = Seq("C")) { df => 
      df.columns.toSeq must contain(exactly("C", "B_1", "B_2", "B_4"))
      val result = df.stripCaveats.collect().toSeq
                      .map { row => row.getString(0) -> (row.getString(1), 
                                                         row.getString(2), 
                                                         row.getString(3)) }
                      .toMap
      result must haveSize(5)
      result("1") must beEqualTo( ("3", "2", null) )
      result("2") must beEqualTo( ("4", null, null) )
      result("3") must beEqualTo( ("2", null, null) )
      result("4") must beEqualTo( (null, null, "2") )
      result(null) must beEqualTo( ("2", null, null) )
    }
  }
  "Caveat Multi-Valued Pivots" >> {
    test() { df => 
      val annotations:Row = 
        df.trackCaveats
          .collect()
          .head
          .getAs[Row](Caveats.ANNOTATION_ATTRIBUTE)
          .getAs[Row](Caveats.ATTRIBUTE_FIELD)

      // B_1 has three possible values
      annotations.getAs[Boolean]("B_1") must beTrue
      // B_2 has two possible values, but one is null
      annotations.getAs[Boolean]("B_2") must beFalse
      // B_4 has exactly one possible value
      annotations.getAs[Boolean]("B_4") must beFalse
    }
  }
}
