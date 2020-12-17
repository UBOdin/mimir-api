package org.mimirdb.util

import org.apache.spark.sql.types._

object StringUtils
{
  def uniqueName(name: String, conflicts: Set[String]): String =
  {
    if(conflicts(name)){
      name + "_" + (
        (1 until conflicts.size + 1)
          .find { x => !conflicts(s"${name}_${x}") }
      )
    } else { name }
  }

  def withDefiniteArticle(str: String): String =
  {
    val firstLetter = str.toLowerCase()(0)
    if(Set('a', 'e', 'i', 'o', 'u') contains firstLetter){
      return "an "+str
    } else {
      return "a "+str
    }
  }

  def pluralize(str: String, count: Int) =
    if(count == 1){ str }
    else { plural(str) }

  def plural(str: String): String = 
    str.toLowerCase match {
      case "copy" => str.substring(0, 3)+"ies"
      case _ => str+"s"
    }

  def friendlyTypeString(dataType: DataType): String = 
    dataType match {
      case StringType => "string"
      case IntegerType => "4 byte integer"
      case LongType => "8 byte integer"
      case ShortType => "8 byte integer"
      case FloatType => "single precision float"
      case DoubleType => "double precision float"
      case ArrayType(elem, _) => "array of "+plural(friendlyTypeString(elem))
      case _ => dataType.json
    }
}