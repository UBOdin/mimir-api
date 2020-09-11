package org.mimirdb.data

import play.api.libs.json.{ JsValue, Format }
import org.apache.spark.sql.{ SparkSession, DataFrame } 
import org.mimirdb.profiler.DataProfiler

/**
 * A generic view definition for the Mimir Catalog.  Used by Catalog.put and related operations
 */
trait DataFrameConstructor 
{
  /**
   * Construct the DataFrame represented by this Constructor
   * 
   * @param spark     The Spark session in the context of which to create the DataFrame
   * @param context   Lazy constructors for dependent dataframes, organized by name.  
   * @return          The DataFrame
   *
   * When `Catalog.put` is called, one of its parameters is a list of dependencies.  Only dependent
   * tables will be present in the context.
   */
  def construct(spark: SparkSession, context: Map[String,() => DataFrame]): DataFrame

  /**
   * The companion object including a deserialization method.
   * @return          The class of companion object extending the DataFrameConstructorCodec trait
   *
   * By default, this assumes a companion object of the same name as the base class.
   */
  def deserializer = getClass.getName + "$"
}

trait DataFrameConstructorCodec
{
  def apply(j: JsValue): DataFrameConstructor
}