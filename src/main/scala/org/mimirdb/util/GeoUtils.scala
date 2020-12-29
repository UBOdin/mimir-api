/***
The origin of much of the Polyline code here in GeoUtils.scala is:

https://github.com/trifectalabs/polyline-scala/blob/master/src/main/scala/Polyline.scala
 
Copyright (c) 2015 Josiah Witt, Christopher Poenaru

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE. 
 */
package org.mimirdb.util

import scala.math.BigDecimal.RoundingMode

case class LatLng(
  lat: Double,
  lng: Double
)

object Polyline {
  implicit def double2BigDecimal(d: Double): BigDecimal = BigDecimal(d)
  implicit def bigDecimal2Double(bd: BigDecimal): Double = bd.toDouble

  def encode(coordinates: List[LatLng]): String = {
    coordinates.foldLeft[List[(BigDecimal,BigDecimal)]](Nil)({(acc, coordinate) =>
      val lat = coordinate.lat.setScale(5, RoundingMode.HALF_EVEN)
      val lng = coordinate.lng.setScale(5, RoundingMode.HALF_EVEN)
      acc match {
        case Nil => List((lat, lng))
        case differences =>
          val currentPos = differences.reduceLeft((pos, diff) => (pos._1 + diff._1, pos._2 + diff._2))
          (lat - currentPos._1, lng - currentPos._2)::differences
      }
    }).reverse.map{ case (latDiff, lngDiff) =>
      encodeDifference(latDiff) + encodeDifference(lngDiff)
    }.mkString
  }

  private def encodeDifference(diff: BigDecimal): String = {
    val value = if (diff < 0) {
      ~((diff * 100000).toInt << 1)
    } else {
      (diff * 100000).toInt << 1
    }
    if (diff == 0)
      encodeFiveBitComponents(value, "") + "?"
    else
      encodeFiveBitComponents(value, "")
  }

  private def encodeFiveBitComponents(value: Int, str: String): String = {
    if (value != 0) {
      val fiveBitComponent = if (value >= 0x20) {
        ((value & 0x1f) | 0x20) + 63
      } else {
        (value & 0x1f) + 63
      }
      encodeFiveBitComponents(value >> 5, str + fiveBitComponent.toChar)
    } else {
      str
    }
  }

  def decode(polyline: String): List[LatLng] = {
    decodeDifferences(polyline, Nil).foldRight[List[LatLng]](Nil)({(diff, acc) =>
      acc match {
        case Nil => List(LatLng(diff._1, diff._2))
        case coordinates => LatLng(
          (coordinates.head.lat + diff._1).setScale(5, RoundingMode.HALF_DOWN),
          (coordinates.head.lng + diff._2).setScale(5, RoundingMode.HALF_DOWN))::coordinates
      }
    }).reverse
  }

  private def decodeDifferences(polyline: String, differences: List[(BigDecimal, BigDecimal)]): List[(BigDecimal, BigDecimal)] = {
    if (polyline.length > 0) {
      val (latDiff, pl1) = decodeDifference(polyline)
      val (lngDiff, pl2) = decodeDifference(pl1)
      decodeDifferences(pl2, (BigDecimal(latDiff/100000.0), BigDecimal(lngDiff/100000.0))::differences)
    } else {
      differences
    }
  }

  private def decodeDifference(polyline: String, shift: Int = 0, result: Int = 0): (Int, String) = {
    val byte = polyline(0).toInt - 63
    val newResult = result | ((byte & 0x1f) << shift)
    if (byte >= 0x20) {
      decodeDifference(polyline.drop(1), shift+5, newResult)
    } else {
      val endResult =
        if ((newResult & 0x01) == 0x01)
          ~(newResult >> 1)
        else
          (newResult >> 1)
      (endResult, polyline.drop(1))
    }
  }
}