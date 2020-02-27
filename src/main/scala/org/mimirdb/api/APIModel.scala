package org.mimirdb.api

import play.api.libs.json._
import org.apache.spark.sql.types.DataType

case class Tuple (
            /* name */
                  name: String,
            /* value */
                  value: String
)

object Tuple {
  implicit val format: Format[Tuple] = Json.format
}

case class Schema (
            /* name of the element */
                  name: String,
            /* type name of the element */
                  `type`: String,
            /* base type name of the element */
                  baseType: String
) {
  def this(name: String, dt: DataType) = {
    this(name, dt.typeName, dt.typeName)
  }
  def sparkType: DataType = 
  {
    DataType.fromJson("\""+`type`+"\"")
  }
}

object Schema {
  implicit val format: Format[Schema] = Json.format
}


case class Repair (
            /* name of selector */
                  selector: String
)

object Repair {
  implicit val format: Format[Repair] = Json.format
}
