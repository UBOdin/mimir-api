package org.mimirdb.rowids

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions._

object MergeRowIds
{
  def apply(exprs:Expression*): Expression = 
    exprs match {
      case Seq() => Literal(1l)
      case Seq(singleton) => Cast(singleton, LongType)
      case _ => new Murmur3Hash(exprs)
    }

  def apply(name: String, id: ExprId, exprs: Expression*): NamedExpression = 
    Alias(apply(exprs:_*), name)(id)
}