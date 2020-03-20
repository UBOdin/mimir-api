package org.mimirdb.lenses.implementation 

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.Lens
import org.mimirdb.spark.SparkPrimitive.dataTypeFormat
import org.mimirdb.spark.expressionLogic.foldOr


case class MergeAttributesLensConfig(
  inputs: Seq[String],
  output: String,
  dataType: Option[DataType]
)

object MergeAttributesLensConfig
{
  implicit val format:Format[MergeAttributesLensConfig] = Json.format
}

object MergeAttributesLens
  extends Lens
{
  def train(input: DataFrame, rawConfig: JsValue): JsValue = 
  {
    val fields:Set[String] = input.schema.fieldNames.map { _.toLowerCase }.toSet
    val config = rawConfig.as[MergeAttributesLensConfig]
    val inputs:Set[String] = config.inputs.map { _.toLowerCase }.toSet
    val conflicts:Seq[String] = 
      (inputs -- fields).map { "Missing attribute "+_ }.toSeq ++
        (if(fields(config.output) && !inputs(config.output)){ 
          Some(s"Attribute ${config.output} already exists") 
        } else { None })
    if(!conflicts.isEmpty) {
      val plural = if(conflicts.size > 1){ "s" } else {""}
      throw new IllegalArgumentException(
        s"Error$plural creating merge attributes lens: ${conflicts.mkString(",")}")
    }
    Json.toJson(config)
  }

  def create(input: DataFrame, rawConfig: JsValue, context: String): DataFrame = 
  {
    val config = rawConfig.as[MergeAttributesLensConfig]
    val isMergeField = config.inputs.map { _.toLowerCase }.toSet
    val castIfNeeded = 
      config.dataType
            .map { dt => { in:Column => in.cast(dt) } }
            .getOrElse { in:Column => in }

    val fieldsToMerge: Seq[Column] =
      config.inputs.map { field => castIfNeeded(input(field)) }

    val firstNonNullValue = 
      fieldsToMerge.foldLeft(null:Column) { 
        case (null, field) => when(not(isnull(field)), field)
        case (prev, field) => prev.when(not(isnull(field)), field)
      }.otherwise(lit(null))
    
    val caveatIsNeeded = (
      fieldsToMerge.foldLeft(Seq[(Column,Seq[Column])]()) {
        (accum, field) => 
          accum.map { 
            case (testField, comparisonFields) => 
              testField -> (comparisonFields :+ field)
          } :+ (field, Seq())
      }
      // Now we have a bunch of expressions of the form 
      // column -> every subsequent column
      // We use this to find the first non-null column and then
      // compare it to every subsequent non-null column.
      .foldLeft(null:Column) {
        case (null, (testField, comparisonFields)) => 
          when(
            not(isnull(testField)), 
            comparisonFields.map { isnull(_) }
                            .fold(lit(false)) { (_:Column) or (_:Column) }
            or comparisonFields.map { testField =!= _ }
                               .fold(lit(false)) { (_:Column) or (_:Column) }
          )
        case (prev, (testField, comparisonFields)) => 
          prev.when(
            not(isnull(testField)), 
            comparisonFields.map { isnull(_) }
                            .fold(lit(false)) { (_:Column) or (_:Column) }
            or comparisonFields.map { testField =!= _ }
                               .fold(lit(false)) { (_:Column) or (_:Column) }
          )
      }.otherwise(lit(false)) // otherwise everything is null
    )

    def castToString(col: Column) =
      when(isnull(col), lit("<null>"))
        .otherwise(col.cast(StringType))

    val mergedAttribute =
      firstNonNullValue.caveatIf(
        concat(
          lit(s"Mismatch in $context.  Merging: "), 
          fieldsToMerge.tail.fold(castToString(fieldsToMerge.head)) { 
            (lhs, rhs) => concat(lhs, lit(" ~ "), castToString(rhs))
          }
        ),
        caveatIsNeeded
      )

    
    val appendingOutputFieldIfNeeded = 
      if(input.schema.fieldNames.exists { _.equalsIgnoreCase(config.output) }) { None }
      else { Some(mergedAttribute.as(config.output)) }


    val outputFields = 
      input.schema
           .fieldNames
           .flatMap { 
             case field if field.equalsIgnoreCase(config.output) => {
                Some(mergedAttribute.as(field))
             }
             case field if isMergeField(field.toLowerCase) => None
             case field => Some(input(field))
           } ++ appendingOutputFieldIfNeeded


    input.select(outputFields:_*)
  }
}