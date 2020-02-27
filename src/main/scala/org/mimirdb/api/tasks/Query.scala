package org.mimirdb.api.tasks

import org.mimirdb.api.{ DataContainer, Schema } 

object Query
{
  def apply(
    query: String,
    includeUncertainty: Boolean
  ): DataContainer = ???

  def getSchema(
    query: String
  ): Seq[Schema] = ???

}