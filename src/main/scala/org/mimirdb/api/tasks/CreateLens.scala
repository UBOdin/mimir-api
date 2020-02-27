package org.mimirdb.api.tasks

import org.apache.spark.sql.SparkSession
import org.mimirdb.api.MimirAPI
import play.api.libs.json.JsValue

object CreateLens
{
  def supportedLenses = Seq[String]()

  def apply(
    /* input for lens */
      input: String,
    /* configuration parameters */
      params: JsValue,
    /* type name of lens */
      t: String,
    /* materialize input before creating lens */
      materialize: Boolean,
    /* optional human-readable name */
      humanReadableName: Option[String],
    /* optional name for the result table */
      resultName: Option[String],

      sparkSession: SparkSession = MimirAPI.sparkSession
  ): String = ???
}