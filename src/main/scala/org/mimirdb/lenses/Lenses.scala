package org.mimirdb.lenses

object Lenses
{
  val implementations = scala.collection.mutable.Map[String, Lens](

  )

  def supportedLenses = implementations.keys.toSeq

  def apply(lens: String) = implementations(lens)
}