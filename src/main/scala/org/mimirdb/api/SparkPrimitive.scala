package org.mimirdb.api

import play.api.libs.json._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import java.util.{ Base64, Calendar }
import java.sql.{ Date, Timestamp }

object SparkPrimitive
{
  def base64Encode(b: Array[Byte]): String =
    Base64.getEncoder().encodeToString(b)

  def base64Decode(b: String): Array[Byte] =
    Base64.getDecoder().decode(b)

  def formatDate(date: Date): String = {
    val cal = Calendar.getInstance();
    cal.setTime(date)
    val y = cal.get(Calendar.YEAR)
    val m = cal.get(Calendar.MONTH)
    val d = cal.get(Calendar.DAY_OF_MONTH)
    f"$y%04d-$m%02d-$d%02d"
  }

  def formatTimestamp(timestamp: Timestamp): String = {
    val cal = Calendar.getInstance()
    cal.setTime(timestamp)
    val y   = cal.get(Calendar.YEAR)
    val m   = cal.get(Calendar.MONTH)
    val d   = cal.get(Calendar.DAY_OF_MONTH)
    val hr  = cal.get(Calendar.HOUR_OF_DAY)
    val min = cal.get(Calendar.MINUTE)
    val sec = cal.get(Calendar.SECOND)
    val ms  = cal.get(Calendar.MILLISECOND)
    f"$y%04d-$m%02d-$d%02d $hr%02d:$min%02d:$sec%02d.$ms%04d"
  }

  def encode(k: Any, t: DataType): JsValue =
  {
    t match {
      case StringType           => JsString(k.toString)
      case BinaryType           => JsString(base64Encode(k.asInstanceOf[Array[Byte]]))
      case BooleanType          => JsBoolean(k.asInstanceOf[Boolean])
      case DateType             => JsString(formatDate(k.asInstanceOf[Date]))
      case TimestampType        => JsString(formatTimestamp(k.asInstanceOf[Timestamp]))
      case CalendarIntervalType => JsString(k.asInstanceOf[CalendarInterval].toString)
      case DoubleType           => ???
      case FloatType            => ???
      case ByteType             => ???
      case IntegerType          => ???
      case LongType             => ???
      case ShortType            => ???
      case NullType             => JsNull
      case _                    => ???
    }
  }
  def decode(k: JsValue, t: DataType): Any = ???
}
