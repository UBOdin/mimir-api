package org.mimirdb.api

import play.api.libs.json._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{ DataType, ArrayType, StructField, StructType }
import org.apache.spark.sql.geosparksql.UDT.GeometryUDT
import org.apache.spark.sql.types.UDTRegistration
import org.apache.spark.sql.SqlUDTRegistrationProxy

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
                  t: DataType,
) {
  override def toString: String =
    s"[$name:${t.typeName}]"
}

object Schema {
  def apply(df: DataFrame): Seq[Schema] =
    apply(df.schema)

  def apply(schema: StructType): Seq[Schema] =
    schema.fields.map { Schema(_) }

  def apply(field: StructField): Schema =
    Schema(field.name, field.dataType)

  def apply(name: String, t: String): Schema = 
    Schema(name, decodeType(t))

  def decodeType(t: String): DataType =
    t match  {
      case "geometry" => 
        SqlUDTRegistrationProxy.getUDT(t)
      case _ if t.startsWith("array:") => 
        ArrayType(decodeType(t.substring(6)))
      case _ => 
        DataType.fromJson("\""+t+"\"")
    }

  def encodeType(t: DataType): String =
    t match {
      case ArrayType(element, _) => s"array:${encodeType(element)}"
      case _ => t.typeName
    }

  implicit val format = Format[Schema](
    new Reads[Schema] { 
      def reads(j: JsValue): JsResult[Schema] = 
      {
        val fields = j.as[Map[String, JsValue]]
        return JsSuccess(Schema(
          fields
            .get("name")
            .getOrElse { return JsError("Expected name field") }
            .as[String],
          decodeType(
            fields
              .get("type")
              .getOrElse { return JsError("Expected type field") }
              .as[String] 
          )
        ))
      }
    }, 
    new Writes[Schema] {
      def writes(s: Schema): JsValue =
      {
        val t = encodeType(s.t)
        Json.obj(
          "name" -> s.name,
          "type" -> t,
          "baseType" -> t
        )
      }
    }
  )
}


case class Repair (
            /* name of selector */
                  selector: String
)

object Repair {
  implicit val format: Format[Repair] = Json.format
}
