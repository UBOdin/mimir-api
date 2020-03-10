package org.mimirdb.data

import org.specs2.mutable.Specification
import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.request.LoadConstructor

class CatalogSpec 
  extends Specification
  with SharedSparkTestInstance
{

  lazy val catalog = new Catalog("target/catalog.db", spark)

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

      reloaded must be equalTo(reloaded)
    }

  }

}