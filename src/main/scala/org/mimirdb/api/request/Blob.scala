package org.mimirdb.api.request

import play.api.libs.json._
import org.mimirdb.api.{ Request, MimirAPI, FormattedError, BytesResponse }
import org.mimirdb.blobs.BlobStore
import java.io.InputStream
import scala.collection.mutable
import org.mimirdb.util.StreamUtils
import javax.servlet.http.HttpServletRequest


case class CreateBlobRequest(
  stream: InputStream,
  blobType: Option[String],
  id: Option[String] = None
) extends Request {
  val data = StreamUtils.readAll(stream)
  val resolvedBlobType = blobType.getOrElse { "text/plain" }
  val resolvedId = 
    id.getOrElse {
      resolvedBlobType.toUpperCase()+"_" + data.hashCode().toString.replace("-", "")
    }

  def handle: CreateBlobResponse = {
    MimirAPI.blobs.put(
      resolvedId,
      resolvedBlobType,
      data
    )
    CreateBlobResponse(resolvedId)
  }
}
object CreateBlobRequest
{
  def apply(req: HttpServletRequest): CreateBlobRequest =
    CreateBlobRequest(req.getInputStream(), req.getParameterValues("type").headOption, None)
  def apply(req: HttpServletRequest, id: String): CreateBlobRequest =
    CreateBlobRequest(req.getInputStream(), req.getParameterValues("type").headOption, Some(id))
}

case class CreateBlobResponse(
  id: String
) extends BytesResponse
{
  def getBytes = id.getBytes()
}

case class GetBlobRequest(
                  id: String,
) extends Request {
  def handle: GetBlobResponse = {
    val (t, data) = 
      MimirAPI.blobs.get(id).getOrElse {
        throw FormattedError(null, s"Blob $id does not exist")
      }
    GetBlobResponse(data, t)
  }
}

case class GetBlobResponse(
  data: Array[Byte],
  blobType: String
) extends BytesResponse
{
  override def contentType = blobType
  def getBytes = data
}