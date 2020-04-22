package org.apache.spark

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.UDTRegistration

object UDTRegistrationProxy {

  def getUDT(userClassName:String) : Class[_] = {
    if(UDTRegistration.exists(userClassName)){
      UDTRegistration.getUDTFor(userClassName).get
    }
    else throw new Exception(s"unknown UDT: $userClassName")

  }

}
