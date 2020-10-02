package org.mimirdb.data

import org.apache.spark.sql.{ Row, DataFrame }
import org.mimirdb.rowids.AnnotateWithSequenceNumber
import scala.collection.concurrent.TrieMap

/**
 * A simple LRU cache for dataframes
 * 
 * Typical interactions are: 
 *   DataFrameCache(tableName, df)(start, end)
 */
object DataFrameCache
{
  /**
   * The number of rows in a buffer page
   */
  val BUFFER_PAGE = {
    val str = System.getenv("DATAFRAME_CACHE_PAGE_ROWS")
    if(str.equals("")) { 10000 } else { str.toInt }
  }
  /**
   * The number of pages cached before we start evictions
   */
  val BUFFER_SIZE = {
    val str = System.getenv("DATAFRAME_CACHE_PAGE_SIZE")
    if(str.equals("")) { 100 } else { str.toInt }
  }

  /**
   * The actual cache
   */
  private val cache = TrieMap[String, CachedDataFrame]()

  /**
   * The user-facing lookup function
   */
  def apply(table: String, df: DataFrame): CachedDataFrame =
  {
    cache.getOrElseUpdate(table, { new CachedDataFrame(df) } )
  }

  /**
   * Align start/end to page boundaries
   *
   * TODO: Enable some sort of prefetching, by expanding the range 
   * if start is close to end?
   */
  def alignRange(start: Int, end: Int): (Int, Int) =
  {
    (
      start - (start % BUFFER_PAGE), 

      end + (if(end % BUFFER_PAGE == 0){ 0 }
             else { (BUFFER_PAGE - (end % BUFFER_PAGE)) })
    )
  }

  /**
   * Check if the buffer is too big and release cache entries if so.
   *
   * The `target` parameter is guaranteed to remain in the cache
   * after this is invoked
   */
  def updatePageCount(target: CachedDataFrame)
  {
    // Tally up the number of pages allocated in the cache
    val pages = cache.values.map { _.pages }.sum

    // If we need to free...
    if(pages > BUFFER_SIZE){
      var freed = 0
      // Simple LRU: Iterate over the elements in order of last access
      for((table, entry) <- cache.toSeq.sortBy { -_._2.lastAccessed }){
        // Keep freeing until we've freed enough pages.
        if(pages - freed > BUFFER_SIZE){ 
          // Make sure that we don't free the one thing that we're updating
          if(entry != target){
            // Remove the table from the cache
            cache.remove(table)
            // And register the pages that we've freed
            freed += entry.pages
          }
        }
      }
    }
    // In the unlikely event that we were asked to buffer more than the buffer
    // size, it'll still be in the cache... but the buffer size should be big
    // enough to prevent that.
  }

  def countPages(start: Int, end: Int): Int =
  {
    ((end - start) / BUFFER_PAGE) + 
      (if((end-start) % BUFFER_PAGE > 0) { 1 } else { 0 })
  }
}


class CachedDataFrame(df: DataFrame)
{
  var lastAccessed = System.currentTimeMillis()
  var bufferStart = 0
  var buffer: Array[Row] = Array()
  
  def bufferEnd = bufferStart + buffer.size

  def apply(start: Int, end: Int): Array[Row] = 
  {
    if(start < bufferStart || end >= bufferEnd){ 
      rebuffer(start, end)
    }
    lastAccessed = System.currentTimeMillis()
    buffer.slice(start - bufferStart, end - bufferEnd)
  }

  def rebuffer(start: Int, end: Int)
  {
    val (targetStart, targetEnd) = DataFrameCache.alignRange(start, end)
    DataFrameCache.updatePageCount(this)
    buffer = df.filter(
                  (df(AnnotateWithSequenceNumber.ATTRIBUTE) >= targetStart)
                    and
                  (df(AnnotateWithSequenceNumber.ATTRIBUTE) < targetEnd)
                )
               .collect()
    bufferStart = targetStart
  }

  def pages = DataFrameCache.countPages(bufferStart, bufferEnd)
}
