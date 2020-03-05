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
  def sparkType: DataType = 
    DataType.fromJson("\""+`type`+"\"")
  override def toString: String =
    s"[$name:${`type`}]"
}

object Schema {
  def apply(name: String, t: String): Schema = 
    Schema(name, t, t)
  def apply(name: String, dt: DataType): Schema = 
    Schema(name, dt.typeName, dt.typeName)
  implicit val format: Format[Schema] = Json.format
}


case class Repair (
            /* name of selector */
                  selector: String
)

object Repair {
  implicit val format: Format[Repair] = Json.format
}
