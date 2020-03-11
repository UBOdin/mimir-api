package org.mimirdb.lenses

import org.mimirdb.lenses.implementation._

object Lenses
{
  val implementations = scala.collection.mutable.Map[String, Lens](
    "INFER_TYPES" -> TypeInferenceLens
  )

  def supportedLenses = implementations.keys.toSeq

  def apply(lens: String) = implementations(lens)
}