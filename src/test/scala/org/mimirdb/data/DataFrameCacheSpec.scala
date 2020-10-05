package org.mimirdb.data

import org.specs2.mutable.Specification
import org.specs2.specification.BeforeAll
import org.mimirdb.api.{ MimirAPI, SharedSparkTestInstance }
import org.mimirdb.util.TimerUtils

class DataFrameCacheSpec
  extends Specification
  with SharedSparkTestInstance
{

  "Improve dataframe performance on small tables" >> {
    val test_r = spark.read
                      .option("header", "true")
                      .csv("test_data/r.csv")
    val test_r_cache = 
      DataFrameCache("TEST_R_PERFORMANCE") { test_r }

    val (coldRows, coldTime) = 
      TimerUtils.time { test_r.collect().map { r => (r.getAs[Int](0), r.getAs[Int](1), r.getAs[Int](2))} }
    println(s"Cold Access ($test_r): ${coldTime/1000000000.0} s")
    val (hotRows, hotTime) = 
      TimerUtils.time { test_r.collect().map { r => (r.getAs[Int](0), r.getAs[Int](1), r.getAs[Int](2))} }
    println(s"Hot Access ($test_r): ${hotTime/1000000000.0} s")
    hotRows must beEqualTo(coldRows)
    hotTime should beLessThan(coldTime)

    // warm up the cache:
    val (coldCacheRows, coldCacheTime) = 
      TimerUtils.time { test_r_cache(0, coldRows.size).map { r => (r.getAs[Int](0), r.getAs[Int](1), r.getAs[Int](2))} }

    println(s"Cold Cache Access ($test_r): ${coldCacheTime/1000000000.0} s")
    coldCacheRows must beEqualTo(coldRows)

    val (hotCachedRows, hotCacheTime) = 
      TimerUtils.time { test_r_cache(0, coldRows.size).map { r => (r.getAs[Int](0), r.getAs[Int](1), r.getAs[Int](2))} }
    println(s"Hot Cache Access ($test_r): ${hotCacheTime/1000000000.0} s")

    hotCachedRows must beEqualTo(coldRows)
    hotCacheTime should beLessThan(hotTime)
  }

  "Handle paging correctly" >> {
    val PAGE = DataFrameCache.BUFFER_PAGE
    val seq = DataFrameCache("SEQ_PERFORMANCE"){
                spark.range(PAGE * 10).toDF
              }

    def getRows(start: Long, end: Long) = {
      val rows = seq(start, end) 
      rows must haveSize((end - start).toInt)
      rows.map { _.getAs[Long](0) }.toSeq
    }
    getRows(0, 10) must beEqualTo(0 until 10)
    getRows(0, PAGE) must beEqualTo(0 until PAGE.toInt)

    TimerUtils.time { getRows(0, PAGE) }
              ._2 must beLessThan(10000000l) // 10 ms


    getRows(PAGE, PAGE*2) must beEqualTo(PAGE until (PAGE*2))
    getRows(PAGE/2, (PAGE*3)/2) must beEqualTo((PAGE/2) until ((PAGE*3)/2))
  }
}