package org.mimirdb.util

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
}