package org.mimirdb.api.tasks

import org.apache.spark.sql.{ SparkSession, DataFrame, Row }
import org.apache.spark.sql.types.StructType
import org.mimirdb.api.{ DataContainer, Schema, MimirAPI } 
import org.mimirdb.caveats.implicits._
import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimirdb.caveats.{ Constants => Caveats }

object Query
{
  def apply(
    query: String,
    includeUncertainty: Boolean,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): DataContainer = 
    apply(sparkSession.sql(query), includeUncertainty)

  def apply(
    query: DataFrame,
    includeUncertainty: Boolean
  ): DataContainer =
  {
    var df = query

    /////// We need the schema before any annotations to produce the right outputs
    val schema = getSchema(df)

    /////// Add a __MIMIR_ROWID attribute
    df = AnnotateWithRowIds(df)

    /////// If requested, add a __CAVEATS attribute
    if(includeUncertainty){ df = df.trackCaveats }
    
    /////// Create a mapping from field name to position in the output tuples
    val postAnnotationSchema = 
      getSchema(df)
        .zipWithIndex
        .map { case (attribute, idx) => attribute.name -> idx }
        .toMap

    /////// 
    val fieldIndices = 
      schema.map { attribute => postAnnotationSchema(attribute.name) }
    val identifierAnnotation = postAnnotationSchema(AnnotateWithRowIds.ATTRIBUTE)

    val results = df.cache().collect()

    val (colTaint, rowTaint): (Seq[Seq[Boolean]], Seq[Boolean]) = 
      if(includeUncertainty){
        results.map { row =>
          val annotation = row.getAs[Row](Caveats.ANNOTATION_ATTRIBUTE)
          val columns = annotation.getAs[Row](Caveats.ATTRIBUTE_FIELD)
          (
            schema.map { attribute => row.getAs[Boolean](attribute.name) },
            annotation.getAs[Boolean](Caveats.ROW_FIELD)
          )
        }.toSeq.unzip[Seq[Boolean], Boolean]
      } else { (Seq[Seq[Boolean]](), Seq[Boolean]()) }

    DataContainer(
      schema,
      results.map { row => fieldIndices.map { row.get(_) } }.toSeq,
      results.map { _.getInt(identifierAnnotation).toString }.toSeq,
      colTaint, 
      rowTaint,
      Seq()
    )
  }

  def getSchema(df: DataFrame):Seq[Schema] = 
    df.schema match { 
      case StructType(fields) => 
        fields.map { field => Schema(field.name, field.dataType) }
      case other => 
        throw new IllegalArgumentException(s"Query produces a non-dataframe output $other")
    }

  def getSchema(
    query: String,
    sparkSession: SparkSession = MimirAPI.sparkSession
  ): Seq[Schema] = 
    getSchema(sparkSession.sql(query))

}