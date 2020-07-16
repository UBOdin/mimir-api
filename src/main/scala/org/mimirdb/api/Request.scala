package org.mimirdb.api

import play.api.libs.json._

abstract class Request {
  def handle: Response
}








