package org.mimirdb.rowids

import org.apache.spark.sql.{ SparkSession, DataFrame }
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._

object WithSemiStableIdentifier
{

  def apply(plan: LogicalPlan, identifierAttributeName: String, session: SparkSession, offset:Long = 1): LogicalPlan =
  {
    val PARTITION_ID     = identifierAttributeName+"_PARTITION_ID"
    val PARTITION_OFFSET = identifierAttributeName+"_PARTITION_OFFSET"
    val INTERNAL_ID      = identifierAttributeName+"_INTERNAL_ID"
    val HASH_ID          = identifierAttributeName+"_HASH_ID"

    val planWithPartitionedIdentifierAttributes = 
      Project(
        plan.output ++ Seq(
          Alias(SparkPartitionID(), PARTITION_ID)(),
          Alias(MonotonicallyIncreasingID(), INTERNAL_ID)()
        ),
        plan
      )

    /** 
     * id offset for input rows for a given session (Seq of Integers) 
     * 
     * For each partition, determine the difference between the identifier
     * assigned to elements of the partition, and the true ROWID. This 
     * offset value is computed as:
     *   [true id of the first element of the partition]
     *     - [first assigned id of the partition]
     *
     * The true ID is computed by a windowed aggregate over the counts
     * of all partitions with earlier identifiers. The window includes
     * the count of the current partition, so that gets subtracted off.
     * 
     * The first assigned ID is simply obtained by the FIRST aggregate.
     *   [[ Oliver: Might MIN be safer? ]]
     *
     * Calling this function pre-computes and caches the resulting 
     * partition-id -> offset map.  Because the partition-ids are 
     * sequentially assigned, starting from zero, we can represent the 
     * Map more efficiently as a Sequence.
     * 
     * The map might change every session, so the return value of this 
     * function should not be cached between sessions.
     */
    val planToComputeFirstPerPartitionIdentifier = 
      Project(Seq(
        UnresolvedAttribute(PARTITION_ID), 
        Alias(
          Add(
            Subtract(
              Subtract(
                WindowExpression(
                  AggregateExpression(
                    Sum(UnresolvedAttribute("cnt")),
                    Complete,false),
                  WindowSpecDefinition(
                    Seq(), 
                    Seq(SortOrder(UnresolvedAttribute(PARTITION_ID), Ascending)), 
                    UnspecifiedFrame)
                ), 
                UnresolvedAttribute("cnt")),
              UnresolvedAttribute(INTERNAL_ID)
            ),
            Literal(offset)
          ),"cnt")()
        ), 
        Sort(
          Seq(SortOrder(UnresolvedAttribute(PARTITION_ID), Ascending)), 
          true,   
          Aggregate(
            Seq(UnresolvedAttribute(PARTITION_ID)),
            Seq(
              UnresolvedAttribute(PARTITION_ID), 
              Alias(AggregateExpression(
                  Count(Seq(Literal(1))),Complete,false
                ),"cnt")(), 
              Alias(AggregateExpression(
                  First(UnresolvedAttribute(INTERNAL_ID),Literal(false)),Complete,false
                ),INTERNAL_ID)()
            ),
          planWithPartitionedIdentifierAttributes)
        )
      )
    
    val firstPerPartitionIdentifierMap = 
      new DataFrame(
        session,
        planToComputeFirstPerPartitionIdentifier,
        RowEncoder(planToComputeFirstPerPartitionIdentifier.schema)
      ).cache()
       .collect()
       .map { row => row.getInt(0) -> row.getLong(1) }
       .toMap

    def lookupFirstIdentifier(partition: Expression) =
      ScalaUDF(
        (partitionId:Int) => firstPerPartitionIdentifierMap(partitionId),
        LongType,
        Seq(partition),
        Seq(false),
        Seq(IntegerType),
        Some("FIRST_IDENTIFIER_FOR_PARTITION"), /*name hint*/
        true, /*nullable*/
        true, /*deterministic*/
      )

    Project(
      plan.output :+ Alias(MergeRowIds(
        (If(
          IsNull(UnresolvedAttribute(PARTITION_ID)),
          UnresolvedAttribute(INTERNAL_ID),
          Add(
            UnresolvedAttribute(INTERNAL_ID),
            lookupFirstIdentifier(UnresolvedAttribute(PARTITION_ID))
          ),

        ) +: plan.output):_*
      ), identifierAttributeName)(),
      planWithPartitionedIdentifierAttributes
    )
  }
}