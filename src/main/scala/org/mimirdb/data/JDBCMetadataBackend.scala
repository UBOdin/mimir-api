package org.mimirdb.data

import java.sql._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.typesafe.scalalogging.LazyLogging
import play.api.libs.json._

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.Literal

class JDBCMetadataBackend(val protocol: String, val filename: String)
  extends MetadataBackend
  with LazyLogging
{
  var conn: Connection = null
  var openConnections = 0
  var inliningAvailable = false;

  def driver() = protocol

  val tableSchemas: scala.collection.mutable.Map[String, Seq[(String, DataType)]] = mutable.Map()
  val manyManys = mutable.Set[String]()

  def open() = 
  {
    this.synchronized({
      assert(openConnections >= 0)
      if (openConnections == 0) {
        conn = protocol match {
          case "sqlite" =>
            Class.forName("org.sqlite.JDBC")
            val path = java.nio.file.Paths.get(filename).toString
            var c = java.sql.DriverManager.getConnection("jdbc:sqlite:" + path)
            c

          case x =>
            println("Unsupported backend! Exiting..."); System.exit(-1); null
        }
      }

      assert(conn != null)
      openConnections = openConnections + 1
    })
  }

  def close(): Unit = {
    this.synchronized({
      if (openConnections > 0) {
        openConnections = openConnections - 1
        if (openConnections == 0) {
          conn.close()
          conn = null
        }
      }

      assert(openConnections >= 0)
      if (openConnections == 0) assert(conn == null)
    })
  }

  def query[T](
    query: String, 
    schema: Seq[DataType]
  ): Seq[Seq[Any]] =
  {
    logger.trace(query)
    val stmt = conn.createStatement()
    val results = stmt.executeQuery(query)
    var ret = List[Seq[Any]]()
    val extractRow = () => {
      schema.zipWithIndex.map { 
        case (LongType, i) => results.getLong(i+1)
        case (DoubleType, i) => results.getDouble(i+1)
        case (StringType, i) => results.getString(i+1)
        case (ArrayType(_, _), i) => Json.parse(results.getString(i+1)).as[Seq[JsValue]]
        case (StructType(_), i) => Json.parse(results.getString(i+1)).as[Map[String,JsValue]]
        case (MapType(StringType, _, _), i) => Json.parse(results.getString(i+1)).as[Map[String,JsValue]]
        case _ => throw new IllegalArgumentException(s"Invalid Schema for $query")
      }
    }
    while(results.next){ 
      ret = extractRow() :: ret
    }
    results.close()
    return ret.reverse
  }

  def update(
    update: String,
    args: Seq[Literal] = Seq()
  ) {
    logger.trace(update)
    val stmt = conn.prepareStatement(update)
    for((Literal(v, t), i) <- args.zipWithIndex){
      t match {
        case LongType => stmt.setLong(i + 1, v.asInstanceOf[Long])
        case DoubleType => stmt.setDouble(i + 1, v.asInstanceOf[Double])
        case StringType => stmt.setString(i + 1, v.asInstanceOf[String])
        case ArrayType(_, _) 
           | StructType(_)
           | MapType(StringType, _, _) => stmt.setString(i + 1, v.asInstanceOf[JsValue].toString)
        case _ => throw new IllegalArgumentException(s"Invalid Schema for $update")
      }
    }
    stmt.execute()
    stmt.close()
  }

  def getTableSchema(conn:Connection, table: String): Option[Seq[(String, DataType)]] =
  {
    // Hardcoded table schemas:
    table match {
      case "SQLITE_MASTER" => 
        return Some(Seq(
            ("NAME", StringType),
            ("TYPE", StringType)
          ))
      case _ => ()
    }

    val ret = query(s"PRAGMA table_info('$table')", 
      Seq(LongType, StringType, StringType, LongType, StringType, LongType)
    ).map { row => 
      val name    = row(1).toString.trim.toUpperCase
      val rawType = row(2).toString.trim

      name -> DataType.fromJson("\""+rawType+"\"")
    }

    if(ret.isEmpty) { None } else { Some(ret) }
  }

  val ID_COLUMN = "MIMIR_String"

  def registerMap(category: String, migrations: Seq[MapMigration]): MetadataMap =
  {
    val schema = Metadata.foldMapMigrations(migrations)

    if(schema.exists {
      case (_, LongType 
             | StringType 
             | DoubleType 
             | ArrayType(_, _) 
             | StructType(_)
             | MapType(StringType, _, _)) => false
      case _ => true
    }) { 
      throw new IllegalArgumentException("Unsupported map types")
    }

    // Assert that the backend schema lines up with the target
    // This should trigger a migration in the future, but at least 
    // inject a sanity check for now.
    protocol match {
      case "sqlite" => {
        // SQLite doesn't recognize anything more than the simplest possible types.
        // Type information is persisted but not interpreted, so conn.getMetaData() 
        // is useless for getting schema information.  Instead, we need to use a
        // SQLite-specific PRAGMA operation.
        getTableSchema(conn, category) match {
          case Some(existing) => {

            val existingMapCols = mutable.Set(existing.map(_._1):_*)
            lazy val stmt = conn.createStatement()
            var stmtNeedsClose = false
            var existingAfterMods = existing
            // Checking each migration individually is a bit cumbersome.
            // it might be appropriate to create a "Schema versions" table
            // to streamline the process
            for(step <- migrations) {
              step match {
                case InitMap(schema) => 
                case AddColumnToMap(column, t, defaultMaybe) => 
                  if( !(existingMapCols contains column) ){
                    existingMapCols.add(column)
                    existingAfterMods = existingAfterMods :+ (column -> t)

                    val default = defaultMaybe match {
                      case None => ""
                      case Some(x:String) => s" DEFAULT '${x.replaceAll("'", "''")}'"
                      case Some(x) => s" DEFAULT $x"
                    }
                    val addColumn = 
                      s"ALTER TABLE `$category` ADD COLUMN `$column` ${t.simpleString} $default;"
                    stmt.executeUpdate(addColumn)
                    stmtNeedsClose = true
                  }

              }
            }
            if(stmtNeedsClose){ stmt.close() }

            try {
              assert(existingAfterMods.length == schema.length+1)
              for(((_, e), (_, f)) <- existingAfterMods.zip((ID_COLUMN, StringType) +: schema)) {
                assert(e.equals(f))
              }
            } catch {
              case e:AssertionError => 
                throw new RuntimeException(s"Incompatible map $category (Existing: ${existingAfterMods.tail.mkString(", ")}; Expected: ${schema.mkString(", ")})", e);

            }
          }
          case None => {
            val create = s"CREATE TABLE `$category`("+
              (  
                s"`$ID_COLUMN` string PRIMARY KEY NOT NULL" +:
                schema.map { case (name, t) => s"`$name` `${t.simpleString}"}
              ).mkString(",")+
            ")"
            val stmt = conn.createStatement()
            stmt.executeUpdate(create)
            stmt.close()
          }
        }

      }
    }
    tableSchemas.put(category, schema)
    return new MetadataMap(this, category)
  }

  def keysForMap(category: String): Seq[String] = 
  {
    query(
      s"SELECT ${ID_COLUMN} FROM `$category`", 
      Seq(StringType)
    ) .map { _(0).asInstanceOf[String] }
  }
  def allForMap(category: String): Seq[(String, Seq[Any])] = 
  {
    val fields = tableSchemas.get(category).get
    query(
      "SELECT "+
        (ID_COLUMN+:fields.map { "`"+_._1+"`" }).mkString(",")+
        s" FROM `$category`",
      StringType +: fields.map { _._2 }
    ) .map { row => (row.head.asInstanceOf[String], row.tail) }
  }
  def getFromMap(category: String, resource: String): Option[Metadata.MapResource] =
  {
    val fields = tableSchemas.get(category).get
    query(
      "SELECT "+
        fields.map { "`"+_._1+"`" }.mkString(",")+
        s" FROM `$category` WHERE `$ID_COLUMN` = '$resource'",
      fields.map { _._2 }
    ) .headOption
      .map { (resource, _) }
  }
  def putToMap(category: String, resource: Metadata.MapResource)
  {
    val fields = tableSchemas.get(category).get
    update(
      s"INSERT OR REPLACE INTO `$category`("+
          ( 
            ID_COLUMN +: fields.map { "`"+_._1+"`" }
          ).mkString(",")+
        ") VALUES ("+( 0 until (fields.length+1) ).map { _ => "?" }.mkString(",")+")",
      Literal(resource._1, StringType) +: 
        resource._2.zip(fields.map { _._2 })
                   .map { case (v, t) => Literal(v, t) }
    )
  }
  def rmFromMap(category: String, resource: String)
  {
    update(
      s"DELETE FROM `$category` WHERE `$ID_COLUMN` = ?",
      Seq(Literal(resource, StringType))
    )
  }
  def updateMap(category: String, resource: String, fields: Map[String, Any])
  {
    val schema = tableSchemas.get(category).get.toMap
    if(fields.isEmpty){ logger.warn(s"Updating ${category}.${resource} with no fields."); return; }
    val fieldSeq:Seq[(String, Any)] = fields.toSeq
    update(
      s"UPDATE `$category` SET ${fieldSeq.map { "`"+_._1+"` = ?" }.mkString(", ")} WHERE `$ID_COLUMN` = ?",
      fieldSeq.map { field => Literal(field._2, schema(field._1)) } :+ Literal(resource, StringType)
    )
  }

  def registerManyMany(category: String): MetadataManyMany = 
  {
    // Assert that the backend schema lines up with the target
    // This should trigger a migration in the future, but at least 
    // inject a sanity check for now.
    protocol match {
      case "sqlite" => {
        // SQLite doesn't recognize anything more than the simplest possible types.
        // Type information is persisted but not interpreted, so conn.getMetaData() 
        // is useless for getting schema information.  Instead, we need to use a
        // SQLite-specific PRAGMA operation.
        getTableSchema(conn, category) match {
          case Some(existing) => {
            assert(existing.length == 2)
            assert(existing(0)._1.equals("LHS"))
            assert(existing(0)._2.equals(StringType))
            assert(existing(1)._1.equals("RHS"))
            assert(existing(1)._2.equals(StringType))
          }
          case None => {
            val create = s"CREATE TABLE `$category`(LHS string, RHS string, PRIMARY KEY (LHS, RHS));"
            val stmt = conn.createStatement()
            stmt.executeUpdate(create)
            stmt.close()
          }
        }

      }
    }
    manyManys.add(category)
    return new MetadataManyMany(this, category)

  }
  def addToManyMany(category: String,lhs: String,rhs: String)
  {
    update(
      s"INSERT INTO `$category`(LHS, RHS) VALUES (?, ?)",
      Seq(Literal(lhs, StringType), Literal(rhs, StringType))
    )
  }
  def getManyManyByLHS(category: String,lhs: String): Seq[String] =
  {    
    query(
      s"SELECT RHS FROM `$category` WHERE LHS = '${lhs.replaceAll("'", "''")}'",
      Seq(StringType)
    ) .map { _(0).asInstanceOf[String] }
  }
  def getManyManyByRHS(category: String,rhs: String): Seq[String] = 
  {
    query(
      s"SELECT LHS FROM `$category` WHERE RHS = '${rhs.replaceAll("'", "''")}'",
      Seq(StringType)
    ) .map { _(0).asInstanceOf[String] }
  }
  def rmByLHSFromManyMany(category: String,lhs: String)
  {
    update(
      s"DELETE FROM `$category` WHERE LHS = ?",
      Seq(Literal(lhs, StringType))
    )
  }
  def rmByRHSFromManyMany(category: String,rhs: String)
  {
    update(
      s"DELETE FROM `$category` WHERE RHS = ?",
      Seq(Literal(rhs, StringType))
    )
  }
  def rmFromManyMany(category: String,lhs: String,rhs: String): Unit = 
  {
    update(
      s"DELETE FROM `$category` WHERE LHS = ? AND RHS = ?",
      Seq(Literal(lhs, StringType), Literal(rhs, StringType))
    )
  }
}