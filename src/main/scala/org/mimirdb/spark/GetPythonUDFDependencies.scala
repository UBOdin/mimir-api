package org.mimirdb.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, View }
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.expressions.Expression

object GetPythonUDFDependencies
  extends GetDependencies[String]
{
  val byPlan: PartialFunction[LogicalPlan, Set[String]] = { case plan => Set[String]() }
  val byExpression: PartialFunction[Expression, Set[String]] = {
    case PythonUDF(name, _, _, _, _, _, _) => Set(name)
  }
}
