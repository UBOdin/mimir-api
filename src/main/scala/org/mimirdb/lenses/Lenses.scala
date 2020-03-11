package org.mimirdb.lenses

import org.mimirdb.lenses.implementation._

object Lenses
{
  val implementations = scala.collection.mutable.Map[String, Lens](
    "TYPE_INFERENCE" -> TypeInferenceLens
  )

  def supportedLenses = implementations.keys.toSeq

  def apply(lens: String) = implementations(lens)
}