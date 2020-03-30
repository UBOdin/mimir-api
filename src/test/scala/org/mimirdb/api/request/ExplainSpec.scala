package org.mimirdb.api.request

import play.api.libs.json._
import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll

import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.MimirAPI
import org.mimirdb.rowids.AnnotateWithRowIds

class ExplainSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = {
    SharedSparkTestInstance.initAPI
    SharedSparkTestInstance.loadCSV("TYPED_R", "test_data/r.csv", typeInference = true)
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
    val result = request.handle.as[ExplainResponse]
    result.reasons must haveSize(1)
  }
}