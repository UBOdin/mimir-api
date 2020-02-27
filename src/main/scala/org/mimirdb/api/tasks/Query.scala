package org.mimirdb.api.tasks

import org.mimirdb.api.{ DataContainer, Schema } 

object Query
{
  def apply(
    /* query string - sql */
      query: String,
    /* include taint in response */
      includeUncertainty: Boolean,
    /* include reasons in response */
      includeReasons: Boolean
  ): DataContainer = ???

  def getSchema(
    query: String
  ): Seq[Schema] = ???

}