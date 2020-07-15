package org.mimirdb.api.request

import play.api.libs.json._
import org.mimirdb.api.{ Request, MimirAPI, FormattedError }
import org.mimirdb.blobs.BlobStore


case class CreateBlobRequest(
                  name: Option[String],
                  `type`: String,
                  data: Array[Byte]
) extends Request {
  lazy val realName = 
    name.getOrElse {
      val viewNameBase = (`type`.hashCode().toString + data.hashCode().toString).hashCode()
      "BLOB_" + (viewNameBase.toString().replace("-", ""))
    }

  def handle: JsValue = {
    MimirAPI.blobs.put(realName, `type`, data)
    Json.toJson(CreateBlobResponse(realName))
  }
}
object CreateBlobRequest
{
  implicit val format: Format[CreateBlobRequest] = Json.format
}

case class CreateBlobResponse(
  name: String
)
object CreateBlobResponse
{
  implicit val format: Format[CreateBlobResponse] = Json.format
}

case class GetBlobRequest(
                  name: String
) extends Request {
  def handle: JsValue = {
    val (t, data) = 
      MimirAPI.blobs.get(name).getOrElse {
        throw FormattedError(null, s"Blob $name does not exist")
      }
    Json.toJson(GetBlobResponse(t, data))
  }
}
object GetBlobRequest
{
  implicit val format: Format[GetBlobRequest] = Json.format
}

case class GetBlobResponse(
  `type`: String,
  data: Array[Byte]
)
object GetBlobResponse
{
  implicit val format: Format[GetBlobResponse] = Json.format
}