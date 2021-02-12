package org.mimirdb.api.request

import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.encoders.RowEncoder

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimirdb.util.LoggerUtils
import org.mimirdb.lenses.Lenses
import org.mimirdb.caveats.implicits._
import org.mimirdb.caveats.EnumerableCaveatSet

class ExplainSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = {
    SharedSparkTestInstance.initAPI
    SharedSparkTestInstance.loadCSV("TYPED_R", "test_data/error_r.csv", typeInference = true)
  }

  "ExplainCell" >> {
    "work with type inference" >> {
      val df = AnnotateWithRowIds(MimirAPI.catalog.get("TYPED_R"))
      val firstRowid = 
        df.filter(df("B").isNull)
          .select(df(AnnotateWithRowIds.ATTRIBUTE))
          .take(1)
          .head
          .getLong(0)
  
      val request = ExplainCellRequest(
                      "SELECT * FROM TYPED_R",
                      firstRowid.toString,
                      "B"
                    )
      LoggerUtils.trace(
  				"org.mimirdb.caveats.enumerate.*",
  				"org.mimirdb.lenses.CaveatedCast"
  			){
      val result = Json.toJson(request.handle).as[ExplainResponse]
      result.reasons must haveSize(1)
      result.reasons(0).message must not be null
      }
    }
    "work when there are nulls" >> {
      val STRNUMBER = "STRNUMBER"
      val STRNAME = "STRNAME"
      val CITY = "CITY"
      val STATE = "STATE"
      val LATITUDE = "LATITUDE"
      val LONGITUDE = "LONGITUDE"

      val request = CreateLensRequest(
                      "GEO",
                      Json.obj(
                        "houseColumn"  -> STRNUMBER,
                        "streetColumn" -> STRNAME,
                        "cityColumn"   -> CITY,
                        "stateColumn"  -> STATE      
                      ), 
                      Lenses.geocode,
                      false,
                      Some("NULL TEST"),
                      None,
                      None
                    )
      val response = Json.toJson(request.handle).as[CreateLensResponse]
      val df = AnnotateWithRowIds(MimirAPI.catalog.get(response.name))
      val rows = df.select(df(AnnotateWithRowIds.ATTRIBUTE)).collect
      val firstRowid = rows
          .head
          .getLong(0)
      val secondRowid = rows
          .tail
          .head
          .getLong(0)
      val lastRowid = rows
          .last
          .getLong(0)

      df.showCaveats()

      val explainFirstRequest = ExplainCellRequest(
                s"SELECT * FROM ${response.name}",
                firstRowid.toString,
                "LATITUDE"
              )
      val firstResult = Json.toJson(explainFirstRequest.handle).as[ExplainResponse]
      firstResult.reasons must haveSize(0)

      val explainSecondRequest = ExplainCellRequest(
                s"SELECT * FROM ${response.name}",
                secondRowid.toString,
                "LATITUDE"
              )
      val secondResult = Json.toJson(explainSecondRequest.handle).as[ExplainResponse]
      secondResult.reasons must haveSize(1)
      secondResult.reasons(0).message must not be null




      //the last row LATITUDE (after a null in the middle) should have the same caveat message
      val explainLastRequest = ExplainCellRequest(
                s"SELECT * FROM ${response.name}",
                lastRowid.toString,
                "LATITUDE"
              )
      val lastResult = Json.toJson(explainLastRequest.handle).as[ExplainResponse]
      lastResult.reasons must haveSize(1)
      lastResult.reasons(0).message must not be null
    }
  }
}