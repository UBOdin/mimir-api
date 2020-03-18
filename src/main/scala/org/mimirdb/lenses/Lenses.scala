package org.mimirdb.lenses

import org.mimirdb.lenses.implementation._
import org.mimirdb.data.Catalog

object Lenses
{
  val implementations = scala.collection.mutable.Map[String, Lens](
    "TYPE_INFERENCE" -> TypeInferenceLens,
    "MISSING_KEY"    -> MissingKeyLens
  )

  def supportedLenses = implementations.keys.toSeq

  def apply(lens: String) = implementations(lens)

  def initGeocoding(
    geocoders: Seq[Geocoder], 
    catalog: Catalog, 
    cacheFormat:String = "json",
    label:String = "GEOCODE"
  )
  {
    implementations.put(label, new GeocodingLens(
      geocoders.map { g => g.name -> g }.toMap,
      catalog,
      cacheFormat
    ))
  }
}