package org.mimirdb.api.request


import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.specs2.matcher.MatchResult

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.vizual._
import org.mimirdb.rowids.AnnotateWithRowIds
import ch.qos.logback.core.filter.Filter

class VizualSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  lazy val input = MimirAPI.catalog.get("TEST_R")
  
  def command[R](js: String, in: DataFrame = input)(op: DataFrame => R): R =
    op(ExecOnSpark(in, Seq(Json.parse(js).as[Command])))

  def getRowIds(df: DataFrame): Seq[Long] =
    AnnotateWithRowIds(df)
      .select(col(AnnotateWithRowIds.ATTRIBUTE))
      .collect()
      .map { _.getInt(0).toLong }
      .toSeq

  // "DeleteColumn" >> {
  //   command("""{
  //     "id": "deleteColumn", 
  //     "dataset" : "IGNORE_THIS_PARAMETER",
  //     "column" : 0
  //   }""") { result =>
  //     result.columns.toSet must beEqualTo(Set("B", "C")) 
  //   }
  // }
  // "DeleteRow" >> {
  //   val rowids = getRowIds(input)
  //   rowids must not beEmpty
    
  //   val theChosenOne = rowids.head

  //   command(s"""{
  //     "id": "deleteRow", 
  //     "dataset" : "IGNORE_THIS_PARAMETER",
  //     "row" : $theChosenOne
  //   }""") { result =>
  //     result.count().toInt must beEqualTo(rowids.size - 1)
  //     getRowIds(result) must containTheSameElementsAs(rowids.tail)
  //   }
  // }
  // "InsertColumn" >> {
  //   command("""{
  //     "id": "insertColumn", 
  //     "dataset" : "IGNORE_THIS_PARAMETER",
  //     "name" : "D",
  //     "position" : 3
  //   }""") { result =>
  //     result.columns.toSeq must beEqualTo(Seq("A", "B", "C", "D")) 
  //   }
  // }
  // "InsertRow" >> {
  //   val in = input.select(input("A")).collect().map { _.getString(0) }.toSeq

  //   command("""{
  //     "id": "insertRow",
  //     "dataset" : "IGNORE_THIS_PARAMETER",
  //     "position" : -1
  //   }""") { result =>
  //     result.select(result("A"))
  //           .collect().map { _.getString(0) }
  //           .toSeq must beEqualTo(in :+ null)
  //   }
  //   command("""{
  //     "id": "insertRow",
  //     "dataset" : "IGNORE_THIS_PARAMETER",
  //     "position" : 1
  //   }""") { result =>
  //     val expected = in.head +: null +: in.tail
  //     result.select(result("A"))
  //           .collect().map { _.getString(0) }
  //           .toSeq must beEqualTo(expected)
  //   }
  // }
  // "MoveColumn" >> {
  //   command("""{
  //     "id": "moveColumn", 
  //     "dataset" : "IGNORE_THIS_PARAMETER",
  //     "column" : 0,
  //     "position" : 1
  //   }""") { result => 
  //     result.columns.toSeq must beEqualTo(Seq("B", "A", "C")) 
  //   }
  // }
  // "MoveRow" >> {
  //   val rowids = getRowIds(input)
  //   rowids must not beEmpty
    
  //   val theChosenOne = rowids.head

  //   command(s"""{
  //     "id": "moveRow",
  //     "dataset" : "IGNORE_THIS_PARAMETER",
  //     "row" : $theChosenOne,
  //     "position" : 1
  //   }""") { result =>
  //     getRowIds(result) must beEqualTo(rowids.tail.head +: rowids.head +: rowids.tail.tail)
  //   }
  // }
  // "FilterColumns" >> {
  //   val in = input.select(input("B")).collect().map { _.getString(0) }.toSeq

  //   command("""{
  //     "id": "projection",
  //     "dataset" : "IGNORE_THIS_PARAMETER",
  //     "columns" : [
  //       { "columns_column" : 2, "columns_name" : "Carol" },
  //       { "columns_column" : 1, "columns_name" : "Bob" }
  //     ]
  //   }""") { result => 
  //     result.columns.toSeq must beEqualTo(Seq("Carol", "Bob"))
  //     result.select(result("Bob"))
  //           .collect()
  //           .map { _.getString(0) }
  //           .toSeq must beEqualTo(in)
  //   }
  // }
  // "RenameColumn" >> {
  //   val in = input.select(input("B")).collect().map { _.getString(0) }.toSeq

  //   command("""{
  //     "id": "renameColumn", 
  //     "dataset" : "IGNORE_THIS_PARAMETER",
  //     "column" : 1, 
  //     "name" : "Bob"
  //   }""") { result => 
  //     result.columns.toSeq must beEqualTo(Seq("A", "Bob", "C"))
  //     result.select(result("Bob"))
  //           .collect()
  //           .map { _.getString(0) }
  //           .toSeq must beEqualTo(in)
  //   }
    
  "UpdateCell" >> {

    val castInput = input.select( 
                        input("A").cast("int").as("A"), 
                        input("B").cast("long").as("B"), 
                        input("C") 
                    )

    val rowids = getRowIds(castInput)
    val data = castInput.collect()
    val indexes = castInput.columns
                           .zipWithIndex
                           .toMap

    val in = indexes.mapValues { idx => data.map { _.get(idx) }.toSeq }

    val theChosenOne = rowids.head

    def tryExpression(expr: String, expected: Any, col: String = "A", cast: DataType = null) =
    {
      command(s"""{
        "id": "updateCell", 
        "dataset" : "IGNORE_THIS_PARAMETER",
        "column" : ${indexes(col)}, 
        "row" : $theChosenOne,
        "value" : "$expr"
      }""", castInput) { result => 

        val expectedTail = Option(cast) match {
          case None => in(col).tail
          case Some(StringType) => in(col).tail.map { _.toString() }
          case Some(t) => in(col).tail.map { v => lit(v).cast(t).expr.eval() }
        }

        val out =
          result.select(col)
              .collect()
              .map { _.get(0) }
              .toSeq
        out must beEqualTo(expected +: expectedTail)
      }
    }

    // Literal strings should get interpreted according to the type of the column (if possible)
    tryExpression("42", 42, col = "A") 
    tryExpression("42", 42l, col = "B")
    tryExpression("42", "42", col = "C")

    // The empty string should be identical for string-typed columns, and a null otherwise
    tryExpression("", null, col = "A") 
    tryExpression("", null, col = "B")
    tryExpression("", "", col = "C")

    // Uninterpretable values should force a columnar string cast
    tryExpression("flibble", "flibble", cast = StringType)
 
    // Simple example involving expressions
    tryExpression("=1+1", 2)

    // If the expression type can't be upcast to the column type, try the reverse
    tryExpression("='cookie'", "cookie", cast = StringType)

    // A is an int, B is a long.  the =A should automatically get upcast to a long
    tryExpression("=A", in("A").head.asInstanceOf[Int].toLong, col="B")

    // A is an int, B is a long.  the =B should automatically cast the rest of the column to a long
    tryExpression("=B", in("B").head, cast = LongType)
  }
}