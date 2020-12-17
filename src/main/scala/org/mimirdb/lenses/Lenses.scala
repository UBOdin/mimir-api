package org.mimirdb.lenses

import org.mimirdb.lenses.implementation._
import org.mimirdb.data.Catalog

object Lenses
{
  val typeInference = "type_inference" 
  val missingKey    = "missing_key"    
  val picker        = "picker"         
  val repairKey     = "repair_key"     
  val comment       = "comment"        
  val missingValue  = "missing_value"  
  val pivot         = "pivot"          
  val shred         = "shred" 
  val geocode       = "geocode"
  val shapeWatcher  = "shape_watcher"
  val implementations = scala.collection.mutable.Map[String, Lens](
    typeInference      -> TypeInferenceLens,
    missingKey         -> MissingKeyLens,
    picker             -> MergeAttributesLens,
    repairKey          -> RepairKeyLens,
    comment            -> CommentLens,
    missingValue       -> MissingValueLens,
    pivot              -> PivotLens,
    shred              -> ShredderLens,
    shapeWatcher       -> ShapeWatcherLens
  )

  def supportedLenses = implementations.keys.toSeq

  def apply(lens: String) = implementations(lens)

  def initGeocoding(
    geocoders: Seq[Geocoder], 
    catalog: Catalog, 
    cacheFormat:String = "json",
    label:String = geocode
  )
  {
    implementations.put(label, new GeocodingLens(
      geocoders.map { g => g.name -> g }.toMap,
      catalog,
      cacheFormat
    ))
  }
}