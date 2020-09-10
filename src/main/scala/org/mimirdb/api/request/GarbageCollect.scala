package org.mimirdb.api.request

import org.mimirdb.api.{ Request, Response, MimirAPI }
import org.mimirdb.api.SuccessResponse

case class GarbageCollectRequest() extends Request
{
  def handle = {
    MimirAPI.catalog.populateSpark(forgetInvalidTables = true)
    SuccessResponse(s"Garbage Collection Successful: ${MimirAPI.catalog.cache.size} tables still loaded.")
  }
}