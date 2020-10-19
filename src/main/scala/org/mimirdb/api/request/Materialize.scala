package org.mimirdb.api.request

import play.api.libs.json._

import org.mimirdb.api.Request
import org.mimirdb.api.JsonResponse
import org.mimirdb.api.MimirAPI
import org.mimirdb.data.MaterializeConstructor

case class MaterializeRequest(
  table: String,
  resultName: Option[String]
)
  extends Request
{
  lazy val output = 
    resultName.getOrElse {
      val resultNameBase = table.hashCode()
      "MATERIALIZED_" + (resultNameBase.toString().replace("-", ""))
    }

  def handle: MaterializeResponse = 
  {
    MimirAPI.catalog.put(
      output, 
      MaterializeConstructor(table, MimirAPI.catalog),
      Set(table)
    )

    return MaterializeResponse(output)
  }
}
object MaterializeRequest
{
  implicit val format: Format[MaterializeRequest] = Json.format
}

case class MaterializeResponse (
            /* name of resulting view */
            name: String
) extends JsonResponse[MaterializeResponse]

object MaterializeResponse {
  implicit val format: Format[MaterializeResponse] = Json.format
}

