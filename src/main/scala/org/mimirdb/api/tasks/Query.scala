package org.mimirdb.api.tasks

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.types.StructType
import org.mimirdb.api.{ DataContainer, Schema, MimirAPI } 
import org.mimirdb.implicits._

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
    val schema = getSchema(df)
    if(includeUncertainty){ df = df.trackCaveats }
    val results = df.cache().collect()

    DataContainer(
      schema,
      results.map { _.toSeq },
      ???,
      ???,
      ???,
      ???
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