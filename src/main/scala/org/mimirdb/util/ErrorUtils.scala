package org.mimirdb.util

import org.apache.spark.sql.AnalysisException

object ErrorUtils {

  def prettyAnalysisEror(e: AnalysisException, query: String): String =
  {
    val sb = new StringBuilder(e.message+"\n")
    val queryLines = query.split("\n")

    def normalLine(l: String)    { sb.append(s"    $l\n") }
    def highlightLine(l: String) { sb.append(s">>> $l\n") }

    sb.append("in\n")
    e.line match { 
      case None => queryLines.foreach { normalLine(_) }
      case Some(lineNo) => {
        // The line number we get is 1-based.  query's lines are 0-based.
        if(lineNo > queryLines.size){ 
          sb.append(s"Query Trace Error: Got Line #$lineNo out of ${queryLines.size}")
          queryLines.foreach { normalLine(_) }
        } else {
          if(lineNo > 2){
            normalLine(queryLines(lineNo - 3))
          }
          if(lineNo > 1){
            normalLine(queryLines(lineNo - 2))
          }
          highlightLine(queryLines(lineNo - 1))
          for(startPosition <- e.startPosition){
            sb.append("    ")
            (0 until (startPosition-2)).foreach { sb.append(' ') }
            sb.append("^\n")
          }
          if(queryLines.size > lineNo + 0){
            normalLine(queryLines(lineNo))
          }
          if(queryLines.size > lineNo + 1){
            normalLine(queryLines(lineNo+1))
          }
        }
      }
    }

    sb.toString()
  }

}