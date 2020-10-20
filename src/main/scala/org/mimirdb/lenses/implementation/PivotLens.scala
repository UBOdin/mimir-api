package org.mimirdb.lenses.implementation

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, Row, Column }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.Lens
import org.mimirdb.spark.SparkPrimitive.dataTypeFormat
import com.typesafe.scalalogging.LazyLogging

/**
 * The pivot lens "pivots" a table, modifying rows into columns.
 *
 * For example
 * 
 *  Year  |  Color  | Price
 * -------+---------+--------
 *  2020  |  Blue   |  100k
 *  2019  |  Blue   |  90k
 *  2020  |  Red    |  120k
 *  2019  |  Red    |  130k
 * 
 * Pivoting this with 'year' as a target, 'price' as a value, and 'color' as a key produces:
 * 
 *  Color | Price_2019 | Price_2020
 * -------+------------+------------
 *  Blue  |   90k      |   100k
 *  Red   |   130k     |   120k
 *
 * In summary, every `value` column is split into N copies, where N is the number of distinct
 * values of the `target` column.  For each distinct value of `key`, the lens will emit one row
 * containing the corresponding value of the `value` column at the intersection of the `target` and
 * and `key` values.  If no such rows exist, the cell will be NULL.  If multiple *distinct* values
 * exist, the cell contents will be arbitrary.
 *
 * In either case, if there is not exactly one value that can be placed into a single cell in the
 * pivot table, the cell will be caveated.
 */
case class PivotLensConfig(
  target: String,
  keys: Seq[String],
  values: Seq[String],
  pivots: Option[Seq[String]]
)
{
  def withPivots(newPivots: Seq[String]) = 
    PivotLensConfig(target, keys, values, Some(newPivots))
}

object PivotLensConfig
{
  implicit val format:Format[PivotLensConfig] = Json.format
}

object PivotLens
  extends Lens
  with LazyLogging
{
  val DISTINCT_LIMIT = 50

  def train(input: DataFrame, rawConfig: JsValue): JsValue = 
  {
    var config = rawConfig.as[PivotLensConfig]

    if(config.values.isEmpty){
      throw new IllegalArgumentException(s"Can't pivot without at least one value column specified")
    }

    val pivots = 
      input.select( input(config.target).cast(StringType) )
           .distinct()
           .take(DISTINCT_LIMIT + 1)
           .map { _.getString(0) }
           .toSeq

    if(pivots.size > DISTINCT_LIMIT){
      throw new IllegalArgumentException(s"Can't pivot on a column with more than $DISTINCT_LIMIT values")
    }
    if(pivots.size == 0){
      throw new IllegalArgumentException(s"Can't pivot on an empty column")
    }

    Json.toJson(config.withPivots(pivots))
  }

  def create(input: DataFrame, rawConfig: JsValue, context: String): DataFrame = 
  {
    val config = rawConfig.as[PivotLensConfig]

    val safePivots = 
      config.pivots.toSeq.flatten
        .map { p => p -> p.replaceAll("[^0-9a-zA-Z_]+", "_") }

    val target = input(config.target)

    def selectedValueColumn(v: String, p: String) = s"first_${v}_${p}"
    def countColumn(v: String, p: String)         = s"count_${v}_${p}"

    def keyDescriptionParts = 
      if(config.keys.isEmpty) { Seq() }
      else if(config.keys.size == 1) { Seq(lit(" on row "), col(config.keys.head).cast(StringType))}
      else {
        // Emits ' x < ${key1}, ${key2}, ${key3}, ... >'
        lit(" on row < ") +: 
          config.keys
                .flatMap { k => Seq(lit(", "), lit(s"$k : "), col(k).cast(StringType)) }
                .drop(1) :+
          lit(" >")
      }

    val (pivotColumnValues, pivotColumnCounts, caveatedPivotColumns) = 
      config.values
        .flatMap { valueName =>  
          val value = input(valueName)
          safePivots.map { case (pivot, safePivot) =>
            val valueIfPivotOtherwiseNull =
              when(target.cast(StringType) === pivot, value)
                    .otherwise(lit(null))

            val caveatMessage = concat((Seq[Column](
                when(col(countColumn(valueName, safePivot)) === 0, "No")
                  .otherwise(col(countColumn(valueName, safePivot)).cast(StringType)),
                lit(s" possible values for ${valueName}_${pivot}")
              ) ++ keyDescriptionParts 
                :+ lit(s" $context (pivoted on ${config.target})")
            ):_*) 

            val caveatCondition = 
              col(countColumn(valueName, safePivot)) =!= 1

            (
              first(valueIfPivotOtherwiseNull, ignoreNulls = true)
                .as(selectedValueColumn(valueName, safePivot)), 
              countDistinct(valueIfPivotOtherwiseNull)
                .as(countColumn(valueName, safePivot)), 
              col(selectedValueColumn(valueName, safePivot))
                .caveatIf(caveatMessage, caveatCondition)
                .as(s"${valueName}_${pivot}")
            )
          }
        }
        .unzip3 

    val intermediateColumns = (pivotColumnValues ++ pivotColumnCounts)
    
    logger.debug(s"INTERMEDIATE: ${intermediateColumns.map { "\n    "+_.toString }.mkString}")

    val pivotedInputWithCounts =
      if(config.keys.isEmpty){
        input.agg( intermediateColumns.head, intermediateColumns.tail:_* )
      } else {
        input.groupBy( config.keys.map { input(_) }:_*)
             .agg( intermediateColumns.head, intermediateColumns.tail:_* )
      }


    val outputColumns =
      config.keys.map { pivotedInputWithCounts(_) } ++ 
      caveatedPivotColumns

    logger.debug(s"OUTPUT: ${outputColumns.map { "\n    "+_.toString }.mkString}")

    return pivotedInputWithCounts.select(outputColumns:_*)
  }

}