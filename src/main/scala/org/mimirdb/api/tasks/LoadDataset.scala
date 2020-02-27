package org.mimirdb.api.tasks

import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI

object LoadDataset
{
  def apply(
    /* file url of datasorce to load */
      file: String,
    /* format of file for spark */
      format: String,
    /* infer types in data source */
      inferTypes: Boolean,
    /* detect headers in datasource */
      detectHeaders: Boolean,
    /* optionally provide a name */
      humanReadableName: Option[String],
    /* options for spark datasource api */
      backendOption: Seq[(String,String)],
    /* optionally provide dependencies */
      dependencies: Seq[String],
    /* optionally provide an output name */
      resultName: Option[String],
      sparkSession: SparkSession = MimirAPI.sparkSession
  ): String = ???

}