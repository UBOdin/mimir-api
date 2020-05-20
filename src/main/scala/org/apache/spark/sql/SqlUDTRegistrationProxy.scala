package org.apache.spark.sql

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.geosparksql.UDT.GeometryUDT

object SqlUDTRegistrationProxy {

  def getUDT(sqlTypeName:String): DataType = {
    sqlTypeName match {
      case "geometry" => new GeometryUDT()
      case x => throw new Exception(s"unknown UDT: $x")
    }
  }

}
