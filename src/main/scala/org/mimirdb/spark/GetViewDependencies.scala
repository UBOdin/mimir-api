package org.mimirdb.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, View }
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias

object GetViewDependencies
{
  def apply(df: DataFrame): Set[String] =
    apply(df.queryExecution.analyzed)

  def apply(plan: LogicalPlan): Set[String] =
    plan.collect {
      // case View(table, _, _) => table.identifier.unquotedString
      case UnresolvedRelation(table) => table.last:String
      case SubqueryAlias(identifier, _) => identifier.name // identifier.identifier
    }.toSet
}
