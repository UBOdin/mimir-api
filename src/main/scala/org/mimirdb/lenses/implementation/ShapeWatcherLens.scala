package org.mimirdb.lenses.implementation

import play.api.libs.json._
import org.apache.spark.sql.DataFrame
import org.mimirdb.lenses._
import org.mimirdb.profiler.shape._
import org.mimirdb.caveats.implicits._
import com.typesafe.scalalogging.LazyLogging

case class ShapeWatcherConfig(facets: Seq[Facet])

object ShapeWatcherConfig
{
  implicit val format: Format[ShapeWatcherConfig] = Json.format
}

object ShapeWatcherLens 
  extends Lens
  with LazyLogging
{
  def train(input: DataFrame, rawConfig: JsValue): JsValue =
  {
    val config = rawConfig match {
      case JsNull => ShapeWatcherConfig(Facet.detect(input))
      case _ => rawConfig.as[ShapeWatcherConfig]
    }
    Json.toJson(config)
  }

  def create(input: DataFrame, rawConfig: JsValue, context: String): DataFrame = 
  {
    val config = rawConfig.as[ShapeWatcherConfig]
    config.facets.flatMap { facet => 
      facet.test(input).map { _ -> facet.affectsColumn }
    }.foldLeft(input){ (df:DataFrame, error:(String, Option[String])) => 
      logger.debug(s"Building $df <- $error")
      error match {
        case (msg, None) => df.caveat(msg)
        case (msg, Some(errorColumn)) => 
          df.select(
            df.columns.map { col =>
              if(col.equalsIgnoreCase(errorColumn)){ 
                df(col).caveat(msg).as(col)
              } else { 
                df(col)
              }
            }:_*
          )
      }
    }
  }

}