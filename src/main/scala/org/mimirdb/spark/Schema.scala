package org.mimirdb.spark

import play.api.libs.json._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.sedona_sql.UDT.GeometryUDT
import org.apache.spark.sql.types.UDTRegistration

object Schema {
  def apply(df: DataFrame): Seq[StructField] =
    df.schema.fields

  def apply(name: String, t: String): StructField = 
    StructField(name, decodeType(t))

  def decodeType(t: String): DataType =
    t match  {
      case "varchar" => StringType
      case "int" => IntegerType
      case "real" => DoubleType
      case "geometry" => GeometryUDT
      case _ if t.startsWith("array:") => 
        ArrayType(decodeType(t.substring(6)))
      case _ => 
        DataType.fromJson("\""+t+"\"")
    }

  def encodeType(t: DataType): String =
    t match {
      case ArrayType(element, _) => s"array:${encodeType(element)}"
      case DoubleType => "real"
      case IntegerType => "int"
      case GeometryUDT => "geometry"
      case _ => t.typeName
    }

  implicit val fieldFormat = Format[StructField](
    new Reads[StructField] { 
      def reads(j: JsValue): JsResult[StructField] = 
      {
        val fields = j.as[Map[String, JsValue]]
        return JsSuccess(StructField(
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
    new Writes[StructField] {
      def writes(s: StructField): JsValue =
      {
        val t = encodeType(s.dataType)
        Json.obj(
          "name" -> s.name,
          "type" -> t,
          "baseType" -> t
        )
      }
    }
  )
}