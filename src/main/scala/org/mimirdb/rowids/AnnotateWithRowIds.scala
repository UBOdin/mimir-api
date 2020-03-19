package org.mimirdb.rowids

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.JoinType

import com.typesafe.scalalogging.LazyLogging

object AnnotateWithRowIds
{
  val ATTRIBUTE = "__MIMIR_ROWID"
  val FIELD_TYPE = StructField(ATTRIBUTE, LongType)

  def apply(df: DataFrame, rowIdAttribute: String = ATTRIBUTE): DataFrame =
  {
    val annotatedPlan = 
      new AnnotateWithRowIds(df.queryExecution.sparkSession, rowIdAttribute)(
        df.queryExecution.analyzed
      )
    new DataFrame(
      df.queryExecution.sparkSession,
      annotatedPlan,
      RowEncoder(StructType(df.schema.fields :+ FIELD_TYPE))
    )
  }
}

class AnnotateWithRowIds(
  session: SparkSession,
  rowIdAttribute: String = AnnotateWithRowIds.ATTRIBUTE
)
  extends LazyLogging
{
  /**
   * Return a plan with an additional column containing a unique identifier
   * for each row in the input.  The column will be an Array with a variable
   * number of fields.
   */
  def apply(plan: LogicalPlan): LogicalPlan =
    recur(plan)._1


  def passthrough(plan: LogicalPlan): (LogicalPlan, Attribute) =
  {
    val ret = plan.mapChildren { apply(_) }
    (
      ret, 
      getAnnotation(ret)
    )
  }


  def recur(plan: LogicalPlan): (LogicalPlan, Attribute) = 
  {
    val ret: (LogicalPlan, Attribute) = plan match {

      /*********************************************************/
      case _ if planIsAnnotated(plan) => (plan, getAnnotation(plan))

      /*********************************************************/
      case _:ReturnAnswer => passthrough(plan)

      /*********************************************************/
      case _:Subquery     => passthrough(plan)

      /*********************************************************/
      case Project(
          projectList: Seq[NamedExpression], 
          child: LogicalPlan) => 
      {
        val (rewrite, annotation) = recur(child)
        (
          Project(projectList :+ annotation, rewrite),
          annotation
        )
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
          child: LogicalPlan) => passthrough(plan)

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
      {
        val newAnnotation = annotationAttribute()
        val ret =
          Union(
            children.zipWithIndex.map { case (child, idx) => 
              val (rewrite, annotation) = recur(child)
              annotate(rewrite, newAnnotation.exprId, Literal(idx), annotation)
            }
          )
        (ret, newAnnotation)
      }

      /*********************************************************/
      case Join(
          left: LogicalPlan,
          right: LogicalPlan,
          joinType: JoinType,
          condition: Option[Expression],
          hint: JoinHint
      ) => 
      {
        val (leftRewrite, leftAnnotation) = recur(left)
        val (rightRewrite, rightAnnotation) = recur(right)
        val lhs = 
          Project(
            left.output :+ Alias(leftAnnotation, "LHS_"+rowIdAttribute)(),
            leftRewrite
          )
        val rhs = 
          Project(
            right.output :+ Alias(rightAnnotation, "RHS_"+rowIdAttribute)(),
            rightRewrite
          )
        val newAnnotation = annotationAttribute()

        (
          annotate(
            Project(
              plan.output :+ newAnnotation,
              Join(lhs, rhs, joinType, condition, hint)
            ),
            newAnnotation.exprId,
            UnresolvedAttribute("LHS_"+rowIdAttribute),
            UnresolvedAttribute("RHS_"+rowIdAttribute)
          ), 
          newAnnotation
        )

      }

      /*********************************************************/
      case InsertIntoDir(
          isLocal: Boolean,
          storage: CatalogStorageFormat,
          provider: Option[String],
          child: LogicalPlan,
          overwrite: Boolean) => passthrough(plan)

      /*********************************************************/
      case View(
          desc: CatalogTable, 
          output: Seq[Attribute], 
          child: LogicalPlan) => {
        // Since we're changing the logical definition of the expression,
        // we need to strip the view reference.
        // If the view was created with identifiers, the planIsAnnotated case
        // above will catch it.
        recur(child)
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
          child: LogicalPlan) => passthrough(plan)

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
        annotate(
          Aggregate(
            groupingExpressions,
            aggregateExpressions,
            apply(child)
          ), 
          groupingExpressions:_*
        )
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
        passthrough(plan)

      /*********************************************************/
      case LocalLimit(limitExpr: Expression, child: LogicalPlan) => 
        passthrough(plan)

      /*********************************************************/
      case SubqueryAlias(identifier: AliasIdentifier, child: LogicalPlan) => {
        // strip off the identifier, since we're changing the logical meaning
        // of the plan.
        recur(child)
      }

      /*********************************************************/
      case Sample(
          lowerBound: Double,
          upperBound: Double,
          withReplacement: Boolean,
          seed: Long,
          child: LogicalPlan) => passthrough(plan)

      /*********************************************************/
      case Distinct(child: LogicalPlan) => 
      {
        // The annotation attribute will break the distinct operator, so rewrite
        // it as a deduplicate and annotate that.
        recur(Deduplicate(
          child.output,
          child
        ))
      }

      /*********************************************************/
      case Repartition(
          numPartitions: Int, 
          shuffle: Boolean, 
          child: LogicalPlan) => passthrough(plan)

      /*********************************************************/
      case RepartitionByExpression(
          partitionExpressions: Seq[Expression],
          child: LogicalPlan,
          numPartitions: Int) => passthrough(plan)

      /*********************************************************/
      case OneRowRelation() => annotate(plan, Literal(1))

      /*********************************************************/
      case Deduplicate(keys: Seq[Attribute], child: LogicalPlan) => 
        passthrough(plan)

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

        val newAnnotation = annotationAttribute()
        (
          WithSemiStableIdentifier(leaf, newAnnotation, session),
          newAnnotation
        )
      }
    }
    logger.trace(s"ROWID ANNOTATE\n$plan  ---vvvvvvv---\n$ret\n\n")
      
    return ret
  }

  def planIsAnnotated(plan: LogicalPlan): Boolean =
      plan.output.exists { _.name.equals(rowIdAttribute) }

  def getAnnotation(plan: LogicalPlan): Attribute =
    plan.output.find { _.name.equals(rowIdAttribute) }.get

  def annotationAttribute(id: ExprId = NamedExpression.newExprId): Attribute =
    AttributeReference(
      rowIdAttribute,
      LongType,
      false
    )(id)

  private def annotate(plan: LogicalPlan, fields: Expression*): (LogicalPlan, Attribute) =
  {
    val newAnnotation = annotationAttribute()
    (
      annotate(plan, newAnnotation.exprId, fields:_*),
      newAnnotation
    )
  }


  private def annotate(plan: LogicalPlan, id:ExprId, fields: Expression*): LogicalPlan =
  {
    Project(
      plan.output
          .filter { !_.name.equals(rowIdAttribute) } :+
        MergeRowIds(rowIdAttribute, id, fields:_*),
      plan
    )
  }
}