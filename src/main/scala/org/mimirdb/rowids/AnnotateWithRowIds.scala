package org.mimirdb.rowids

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.JoinType

import com.typesafe.scalalogging.LazyLogging

// import org.mimirdb.spark.expressionLogic.{
//   foldOr, 
//   foldIf,
//   attributesOfExpression,
//   aggregateBoolOr
// }

object AnnotateWithRowIds
{
  val ATTRIBUTE = "__MIMIR_ROWID"

  def apply(df: DataFrame, rowIdAttribute: String = ATTRIBUTE): DataFrame =
  {
    val annotatedPlan = 
      new AnnotateWithRowIds(df.queryExecution.sparkSession, rowIdAttribute)(
        df.queryExecution.analyzed
      )
    new DataFrame(
      df.queryExecution.sparkSession,
      annotatedPlan,
      RowEncoder(annotatedPlan.schema)
    )
  }
}

class AnnotateWithRowIds(
  session: SparkSession,
  rowIdAttribute: String = AnnotateWithRowIds.ATTRIBUTE
)
  extends LazyLogging
{
  val ANNOTATION = UnresolvedAttribute(rowIdAttribute)


  /**
   * Return a plan with an additional column containing a unique identifier
   * for each row in the input.  The column will be an Array with a variable
   * number of fields.
   */
  def apply(plan: LogicalPlan): LogicalPlan =
  {
    val ret: LogicalPlan = plan match {

      /*********************************************************/
      case _ if planIsAnnotated(plan) => plan

      /*********************************************************/
      case _:ReturnAnswer => plan.mapChildren { apply(_) }

      /*********************************************************/
      case _:Subquery     => plan.mapChildren { apply(_) }

      /*********************************************************/
      case Project(
          projectList: Seq[NamedExpression], 
          child: LogicalPlan) => 
      {
        Project(projectList :+ ANNOTATION, apply(child))
      }

      /*********************************************************/
      case Generate(
          generator: Generator,
          unrequiredChildIndex: Seq[Int],
          outer: Boolean,
          qualifier: Option[String],
          generatorOutput: Seq[Attribute],
          child: LogicalPlan) => ???

      /*********************************************************/
      case Filter(
          condition: Expression, 
          child: LogicalPlan) => plan.mapChildren { apply(_) }

      /*********************************************************/
      case Intersect(
          left: LogicalPlan, 
          right: LogicalPlan, 
          isAll: Boolean) => ???

      /*********************************************************/
      case Except(
          left: LogicalPlan, 
          right: LogicalPlan, 
          isAll: Boolean) => ???

      /*********************************************************/
      case Union(children: Seq[LogicalPlan]) => 
        Union(
          children.zipWithIndex.map { case (child, idx) => 
            annotate(apply(child), Literal(idx), ANNOTATION)
          }
        )

      /*********************************************************/
      case Join(
          left: LogicalPlan,
          right: LogicalPlan,
          joinType: JoinType,
          condition: Option[Expression]) => 
      {
        val lhs = 
          Project(
            left.output :+ Alias(ANNOTATION, "LHS_"+rowIdAttribute)(),
            apply(left)
          )
        val rhs = 
          Project(
            right.output :+ Alias(ANNOTATION, "RHS_"+rowIdAttribute)(),
            apply(left)
          )
        annotate(
          Join(lhs, rhs, joinType, condition),
          UnresolvedAttribute("LHS_"+rowIdAttribute),
          UnresolvedAttribute("RHS_"+rowIdAttribute)
        )

      }

      /*********************************************************/
      case InsertIntoDir(
          isLocal: Boolean,
          storage: CatalogStorageFormat,
          provider: Option[String],
          child: LogicalPlan,
          overwrite: Boolean) => plan.mapChildren { apply(_) }

      /*********************************************************/
      case View(
          desc: CatalogTable, 
          output: Seq[Attribute], 
          child: LogicalPlan) => {
        // Since we're changing the logical definition of the expression,
        // we need to strip the view reference.
        // If the view was created with identifiers, the planIsAnnotated case
        // above will catch it.
        apply(child)
      }

      /*********************************************************/
      case With(
          child: LogicalPlan, 
          cteRelations: Seq[(String, SubqueryAlias)]) => ???

      /*********************************************************/
      case WithWindowDefinition(
          windowDefinitions: Map[String, WindowSpecDefinition], 
          child: LogicalPlan) => ???

      /*********************************************************/
      case Sort(
          order: Seq[SortOrder], 
          global: Boolean, 
          child: LogicalPlan) => plan.mapChildren { apply(_) }

      /*********************************************************/
      case Range(
          start: Long,
          end: Long,
          step: Long,
          numSlices: Option[Int],
          output: Seq[Attribute],
          isStreaming: Boolean) => 
      {
        // Use the range identifier itself as the annotation.
        annotate(plan, output(0))
      }

      /*********************************************************/
      case Aggregate(
          groupingExpressions: Seq[Expression],
          aggregateExpressions: Seq[NamedExpression],
          child: LogicalPlan) => 
      {
        // use the grouping attributes as the annotation
        // descend into the children just in case an identifier is needed
        // elsewhere.
        annotate(plan.mapChildren { apply(_) }, groupingExpressions:_*)
      }

      /*********************************************************/
      case Window(
          windowExpressions: Seq[NamedExpression],
          partitionSpec: Seq[Expression],
          orderSpec: Seq[SortOrder],
          child: LogicalPlan) => ???

      /*********************************************************/
      case Expand(
        projections: Seq[Seq[Expression]], 
        output: Seq[Attribute], 
        child: LogicalPlan) => ???

      /*********************************************************/
      case GroupingSets(
          selectedGroupByExprs: Seq[Seq[Expression]],
          groupByExprs: Seq[Expression],
          child: LogicalPlan,
          aggregations: Seq[NamedExpression]) => ???

      /*********************************************************/
      case Pivot(
          groupByExprsOpt: Option[Seq[NamedExpression]],
          pivotColumn: Expression,
          pivotValues: Seq[Expression],
          aggregates: Seq[Expression],
          child: LogicalPlan) => ???

      /*********************************************************/
      case GlobalLimit(limitExpr: Expression, child: LogicalPlan) => 
        plan.mapChildren { apply(_) }

      /*********************************************************/
      case LocalLimit(limitExpr: Expression, child: LogicalPlan) => 
        plan.mapChildren { apply(_) }

      /*********************************************************/
      case SubqueryAlias(identifier: AliasIdentifier, child: LogicalPlan) => {
        // strip off the identifier, since we're changing the logical meaning
        // of the plan.
        apply(child)
      }

      /*********************************************************/
      case Sample(
          lowerBound: Double,
          upperBound: Double,
          withReplacement: Boolean,
          seed: Long,
          child: LogicalPlan) => plan.mapChildren { apply(_) }

      /*********************************************************/
      case Distinct(child: LogicalPlan) => 
      {
        // The annotation attribute will break the distinct operator, so rewrite
        // it as a deduplicate and annotate that.
        apply(Deduplicate(
          child.output,
          child
        ))
      }

      /*********************************************************/
      case Repartition(
          numPartitions: Int, 
          shuffle: Boolean, 
          child: LogicalPlan) => plan.mapChildren { apply(_) }

      /*********************************************************/
      case RepartitionByExpression(
          partitionExpressions: Seq[Expression],
          child: LogicalPlan,
          numPartitions: Int) => plan.mapChildren { apply(_) }

      /*********************************************************/
      case OneRowRelation() => annotate(plan, Literal(1))

      /*********************************************************/
      case Deduplicate(keys: Seq[Attribute], child: LogicalPlan) => 
        plan.mapChildren { apply(_) }

      /*********************************************************/
      case leaf:LeafNode => 
      {
        // Leaf-node fallback if nothing else works:  Add an identifier
        // to every row of the dataset.  The identifier combines the hash of the
        // current row with the indexed position of the row in the dataframe.
        // 
        // This form of identity is stable under appends to the dataframe (and
        // several other forms of mutation), but not universally stable.
        // This is sadly necessary: A purely hash-based approach will duplicate
        // identifiers for identical rows, while a pursely position-based 
        // approach allows an identifier to be re-used for a different row.
        //
        // Ideally we don't do this here, since calling this function requires
        // plan execution.  Instead, it's preferable to manually identify 
        // identifier attributes in the 

        WithSemiStableIdentifier(leaf, rowIdAttribute, session)
      }
    }
    logger.trace(s"ROWID ANNOTATE\n$plan  ---vvvvvvv---\n$ret\n\n")
      
    return ret
  }

  def planIsAnnotated(plan: LogicalPlan): Boolean =
      plan.output.map { _.name }.exists { _.equals(rowIdAttribute) }

  private def annotate(plan: LogicalPlan, fields: Expression*): LogicalPlan =
  {
    Project(
      plan.output
          .filter { !_.name.equals(rowIdAttribute) } :+
        MergeRowIds(rowIdAttribute, fields:_*),
      plan
    )
  }
}