package org.mimirdb.vizual

import org.apache.spark.sql.{ DataFrame, Column }
import org.apache.spark.sql.functions._
import org.mimirdb.data.Catalog
import org.mimirdb.rowids.AnnotateWithRowIds
import org.mimirdb.rowids.AnnotateWithSequenceNumber

object ExecOnSpark
{
  def apply(input: DataFrame, script: Seq[Command]): DataFrame =
    script.foldLeft(input) { apply(_, _) }

  def apply(input: DataFrame, command: Command): DataFrame =
  {
    command match {
      case DeleteColumn(column) => 
        {
          input.select(
            input.columns
                 .filter { !_.equalsIgnoreCase(column) }
                 .map { input(_) }
              :_*
          )
        }
      case DeleteRow(rowid) => 
        {
          AnnotateWithRowIds.withRowId(input) { df => 
            df.filter(col(AnnotateWithRowIds.ATTRIBUTE) =!= lit(rowid))
          }
        }
      case InsertColumn(position, column) => 
        {
          val columns = 
            input.columns
                 .map { input(_) }
                 .toSeq
          val (pre, post):(Seq[Column], Seq[Column]) = 
            position.map { columns.splitAt(_) }
                    .getOrElse { (columns, Seq()) }

          input.select( ((pre :+ lit(null).as(column)) ++ post):_* )
        }
      case InsertRow(position) => 
        {
          if(position < 0){
            input.union(
              input.sqlContext
                   .range(1)
                   .select(
                      input.columns
                           .map { lit(null).as(_) }:_*
                   )
            )
          } else {
            AnnotateWithSequenceNumber.withSequenceNumber(input){ df =>
              val seq = df(AnnotateWithSequenceNumber.ATTRIBUTE)
              val oldRowData =
                input.columns.map { df(_) } :+ 
                  when(seq >= position, seq + 1)
                    .otherwise(seq)
                    .as(AnnotateWithSequenceNumber.ATTRIBUTE)
              val newRowData = 
                input.columns
                     .map { lit(null).as(_) } :+
                        lit(position).as(AnnotateWithSequenceNumber.ATTRIBUTE)
              val newRow = 
                input.sqlContext
                     .range(1)
                     .select(newRowData:_*)

              df.select(oldRowData:_*)
                .union(newRow)
                .sort(col(AnnotateWithSequenceNumber.ATTRIBUTE).asc)
            }
          }
        }
      case MoveColumn(column, position) => 
        {
          val otherColumns = input.columns.filter { !_.equalsIgnoreCase(column) }
          val (pre, post) = otherColumns.splitAt(position)
          val finalSchema = (pre :+ column) ++ post

          input.select( finalSchema.map { input(_) } :_* )
        }
      case MoveRow(row, position) => 
        {
          AnnotateWithRowIds.withRowId(input) { rowDF =>
            val targetDropped = 
              rowDF.filter( rowDF(AnnotateWithRowIds.ATTRIBUTE) =!= row )
            AnnotateWithSequenceNumber.withSequenceNumber(targetDropped){ df =>
              val seq = df(AnnotateWithSequenceNumber.ATTRIBUTE)
              val oldRowData =
                input.columns.map { df(_) } :+ 
                  when(seq >= position, seq + 1)
                    .otherwise(seq)
                    .as(AnnotateWithSequenceNumber.ATTRIBUTE) :+
                  df(AnnotateWithRowIds.ATTRIBUTE)
              val replacedRowData = 
                input.columns
                     .map { rowDF(_) } :+
                        lit(position).as(AnnotateWithSequenceNumber.ATTRIBUTE) :+
                  df(AnnotateWithRowIds.ATTRIBUTE)
              val replacedRow = 
                rowDF.filter( rowDF(AnnotateWithRowIds.ATTRIBUTE) === row )
                     .select(replacedRowData:_*)

              df.select(oldRowData:_*)
                .union(replacedRow)
                .sort(col(AnnotateWithSequenceNumber.ATTRIBUTE).asc)
            }
          }
        }
      case FilterColumns(columns) => 
        {
          input.select(
            columns.map { c => 
              input(c.columns_column).as(c.columns_name)
            }:_*
          )
        }
      case RenameColumn(column, name) => 
        {
          val newSchema: Array[Column] =
            input.columns
                 .map { c => if(c.equalsIgnoreCase(column)) { input(c).as(name) } 
                             else { input(c) } }
          input.select(newSchema:_*)
        }
      case UpdateCell(column, row, value) => 
        {
          AnnotateWithRowIds.withRowId(input) { df =>
            val rowid = df(AnnotateWithRowIds.ATTRIBUTE)
            val columns = 
              input.schema
                   .map { c => 
                     if(c.name.equalsIgnoreCase(column)){
                       when(rowid === lit(row), lit(value).cast(c.dataType))
                         .otherwise(df(c.name))
                         .as(c.name)
                     } else { df(c.name) }
                   } :+ rowid
            df.select(columns:_*)
          }
        }
    }
  }
}