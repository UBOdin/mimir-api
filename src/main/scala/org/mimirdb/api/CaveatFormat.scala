package org.mimirdb.api

import play.api.libs.json._
import org.mimirdb.caveats.{ Caveat }

object CaveatFormat
{
  implicit val caveatFormat: Format[Caveat] = ???
}