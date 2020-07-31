package org.mimirdb.api.request

import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimir.util.LoggerUtils

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
    
    val df = AnnotateWithRowIds(MimirAPI.catalog.get("TYPED_R"))
    val firstRowid = 
      df.select(df(AnnotateWithRowIds.ATTRIBUTE))
        .take(1)
        .head
        .getInt(0)

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
}