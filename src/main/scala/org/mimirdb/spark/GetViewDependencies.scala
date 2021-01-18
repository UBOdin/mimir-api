// package org.mimirdb.spark

// import org.apache.spark.sql.DataFrame
// import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, View }
// import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
// import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
// import org.apache.spark.sql.catalyst.expressions.Expression

// object GetViewDependencies
//   extends GetDependencies[String]
// {
//   val byPlan: PartialFunction[LogicalPlan, Set[String]] = {
//       case UnresolvedRelation(table) => Set[String](table.last)
//       // case SubqueryAlias(identifier, _) => Set[String](identifier.name) // identifier.identifier
//     }
//   val byExpression: PartialFunction[Expression, Set[String]] = { 
//       case expr => Set[String]() 
//     }
// }
