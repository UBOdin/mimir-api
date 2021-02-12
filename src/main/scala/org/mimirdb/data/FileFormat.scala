package org.mimirdb.data


object FileFormat {
  type T = String

  val CSV                    = "csv"
  val JSON                   = "json"
  val XML                    = "com.databricks.spark.xml"
  val EXCEL                  = "com.crealytics.spark.excel"
  val JDBC                   = "jdbc"
  val TEXT                   = "text"
  val PARQUET                = "parquet"
  val PDF                    = "mimir.exec.spark.datasource.pdf"
  val ORC                    = "orc"
  val GOOGLE_SHEETS          = "mimir.exec.spark.datasource.google.spreadsheet"
  val CSV_WITH_ERRORCHECKING = "org.apache.spark.sql.execution.datasources.ubodin.csv"
  val BINARY                 = "binaryFile"
}
