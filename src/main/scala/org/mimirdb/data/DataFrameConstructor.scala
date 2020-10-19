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
   * This function returns a DataFrame intended for computing results, and
   * may materialize intermediates.  If the intent is to do static analysis,
   * use provenance().
   * 
   * When `Catalog.put` is called, one of its parameters is a list of dependencies.  Only dependent
   * tables will be present in the context.
   */
  def construct(spark: SparkSession, context: Map[String,() => DataFrame]): DataFrame

  /**
   * Return the full provenance of the DataFrame.
   * 
   * @param spark     The Spark session in the context of which to create the DataFrame
   * @param context   Lazy constructors for dependent dataframes, organized by name.  
   * @return          The DataFrame
   *
   * This function returns a DataFrame intended for static analysis, and may
   * involve more computation than necessary.  If the intent is to compute
   * values, use construct()
   */
  def provenance(spark: SparkSession, context: Map[String, () => DataFrame]): DataFrame

  /**
   * The companion object including a deserialization method.
   * @return          The class name of an object extending the DataFrameConstructorCodec trait
   *
   * By default, this assumes a companion object of the same name as the base class.
   */
  def deserializer = getClass.getName + "$"
}

trait DataFrameConstructorCodec
{
  def apply(j: JsValue): DataFrameConstructor
}

trait DefaultProvenance
{
  def construct(spark: SparkSession, context: Map[String, () => DataFrame]): DataFrame
  def provenance(spark: SparkSession, context: Map[String, () => DataFrame]): DataFrame =
    construct(spark, context)
}