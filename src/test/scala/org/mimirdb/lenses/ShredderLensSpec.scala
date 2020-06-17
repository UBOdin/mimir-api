package org.mimirdb.lenses

import play.api.libs.json._
import org.specs2.specification.BeforeAll
import org.specs2.mutable.Specification

import org.apache.spark.sql.DataFrame

import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.implementation.{
  ShreddedColumn,
  ShreddingOperation,
  ShredderLensConfig,
  SubstringField
}
import org.mimirdb.lenses.implementation.ExtractedField
import org.mimirdb.lenses.implementation.ExplodedField


class ShredderLensSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{
  def beforeAll = SharedSparkTestInstance.initAPI
  
  lazy val lens = Lenses("SHRED")

  def shred[T](
    file: String, 
    keepOriginalColumns: Boolean = false
  )(
    rules: (String, ShreddingOperation)*
  )(op: DataFrame => T): T =
  {
    val table = "SHRED_"+file.replaceAll("[^a-zA-Z]", "")
    SharedSparkTestInstance.loadText(table, "test_data/"+file)
    val df = dataset(table)
    val config = lens.train(
      df,
      Json.toJson(ShredderLensConfig(
        keepOriginalColumns = keepOriginalColumns,
        shreds = rules.map { case (output, op) => 
          ShreddedColumn(input = "value", output = output, operation = op)
        }
      ))
    )
    op(lens.apply(df, config, s" in $table"))
  }

  "Split by Position" >> {
    shred("bank.txt")(
      "table" -> SubstringField(0, 5),
      "respondent_id" -> SubstringField(6, 16)
    ) { df => 
      df.schema.fieldNames.toSeq must contain(eachOf("table", "respondent_id"))
      val row = df.head()
      row.getString(0) must be equalTo("D1-1 ")
      row.getString(1).toInt must be equalTo(1)
    }
  }

  "Split by Field" >> {
    shred("trace.txt")(
      // Literal tabs
      "start_time" -> ExtractedField("\t", 1),
      "latency" -> ExtractedField("\t", 2),
      // Tabs as interpreted by Spark's Regexp Parser
      "op_type" -> ExtractedField("\\t", 3)
    ) { df =>
      df.schema.fieldNames.toSeq must contain(eachOf("start_time", "latency", "op_type"))
      val row = df.head()
      row.getString(0).toLong must beEqualTo(1589903882239540l)
      row.getString(1).toInt must beEqualTo(1)
      row.getString(2).toInt must beEqualTo(1)
    }
  }

  "Shred by De-Pivoting" >> {
    shred("horiz.txt")(
      "v" -> ExplodedField(" ")
    ) { df => 
      df.collect().map { _.getString(0).toInt }.toSeq must beEqualTo((1 until 7).toSeq)
    }
  }
}
