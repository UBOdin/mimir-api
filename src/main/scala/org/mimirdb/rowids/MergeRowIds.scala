package org.mimirdb.rowids

import org.apache.spark.sql.catalyst.expressions._

object MergeRowIds
{
  def apply(exprs:Expression*): Expression = 
    if(exprs.size == 1) { exprs(0) }
    else { new Murmur3Hash(exprs) }

  def apply(name: String, id: ExprId, exprs: Expression*): NamedExpression = 
    Alias(apply(exprs:_*), name)(id)
}