package org.mimirdb.data

import org.specs2.mutable.Specification
import org.mimirdb.api.SharedSparkTestInstance
import org.mimirdb.api.request.DataContainer
import org.mimirdb.api.request.Query
import play.api.libs.json.JsValue
import org.specs2.specification.BeforeAll

class CatalogSpec 
  extends Specification
  with SharedSparkTestInstance
  with BeforeAll
{

  def beforeAll = SharedSparkTestInstance.initAPI

  lazy val catalog = new Catalog("target/catalog.db", spark)

  def query[T](query: String, includeUncertainty: Boolean = true)
              (op: DataContainer => T): T = 
    op(Query(query, includeUncertainty, sparkSession = spark))
  
  "Serialize and Deserialize Constructors" >>
  {
    val table = "TEMP"
    val original = catalog.put(table,
      LoadConstructor(
        "test_data/r.csv",
        "csv",
        Map(), 
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
        Map(),
      ),
      Set[String]()
    )
    query("SELECT COUNT(*) FROM QUERY_R") { result =>
      result.data.map { _(0) } must beEqualTo(Seq(7))
    } 
  }

  "Profile a catalog table" >>
  {
    val table = "profile_r"
    catalog.put(table,
      LoadConstructor(
        "test_data/r.csv",
        "csv",
        Map("header" -> "true"),
      ),
      Set[String]()
    )
    catalog.profile(table)
    val properties = catalog.getProperties(table)
    properties("count").as[Int] must beEqualTo(7)
    properties("columns")
              .as[Seq[Map[String,JsValue]]]
              .apply(0)
              .apply("column")
              .as[Map[String,JsValue]]
              .apply("name")
              .as[String] must beEqualTo("A")

  }

}