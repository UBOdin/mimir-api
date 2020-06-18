package org.mimirdb.data

import org.specs2.mutable.Specification
import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.request.DataContainer
import org.mimirdb.api.request.Query

class CatalogSpec 
  extends Specification
  with SharedSparkTestInstance
{

  lazy val catalog = new Catalog("target/catalog.db", spark)

  def query[T](query: String, includeUncertainty: Boolean = true)
              (op: DataContainer => T): T = 
    op(Query(query, includeUncertainty, sparkSession = spark))
  
  "The Catalog" >> {

    "Serialize and Deserialize Constructors" >>
    {
      val table = "TEMP"
      val original = catalog.put(table,
        LoadConstructor(
          "test_data/r.csv",
          "csv",
          Map()
        ),
        Set[String]()
      ).collect().map { _.toSeq }

      catalog.flush(table)
      val reloaded = catalog.get(table)
        .collect().map { _.toSeq }

      reloaded must be equalTo(original)
    }
    
    "Query a catalog table" >> 
    {
      val table = "QUERY_R"
      catalog.put(table,
        LoadConstructor(
          "test_data/r.csv",
          "csv",
          Map()
        ),
        Set[String]()
      )
      query("SELECT COUNT(*) FROM QUERY_R") { result =>
        result.data.map { _(0) } must beEqualTo(Seq(7))
      } 
    }
  }

}