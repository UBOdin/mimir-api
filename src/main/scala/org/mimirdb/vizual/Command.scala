package org.mimirdb.vizual

import play.api.libs.json._

sealed trait Command
object Command
{
  implicit val format = Format[Command](
    new Reads[Command]{
      def reads(j: JsValue): JsResult[Command] =
      {
        j.as[Map[String, JsValue]].get("id") match {
          case None => JsError("No 'id' field")
          case Some(JsString(id)) => id.toLowerCase match {
            case "deletecolumn"  => JsSuccess(j.as[DeleteColumn])
            case "deleterow"     => JsSuccess(j.as[DeleteRow])
            case "insertcolumn"  => JsSuccess(j.as[InsertColumn])
            case "insertrow"     => JsSuccess(j.as[InsertRow])
            case "movecolumn"    => JsSuccess(j.as[MoveColumn])
            case "moverow"       => JsSuccess(j.as[MoveRow])
            case "projection"    => JsSuccess(j.as[FilterColumns])
            case "renamecolumn"  => JsSuccess(j.as[RenameColumn])
            case "updatecell"    => JsSuccess(j.as[UpdateCell])
            case _ => JsError("Not a valid Vizier command")
          }
          case Some(_) => JsError("Expecting the 'id' field to be a string")
        }
      }
    },
    new Writes[Command] {
      def writes(c: Command): JsValue = 
      {
        val (cmd, js) = 
          c match {
            case x:DeleteColumn  =>  ("deletecolumn",   Json.toJson(x))
            case x:DeleteRow     =>  ("deleterow",      Json.toJson(x))
            case x:InsertColumn  =>  ("insertcolumn",   Json.toJson(x))
            case x:InsertRow     =>  ("insertrow",      Json.toJson(x))
            case x:MoveColumn    =>  ("movecolumn",     Json.toJson(x))
            case x:MoveRow       =>  ("moverow",        Json.toJson(x))
            case x:FilterColumns =>  ("projection",     Json.toJson(x))
            case x:RenameColumn  =>  ("renamecolumn",   Json.toJson(x))
            case x:UpdateCell    =>  ("updatecell",     Json.toJson(x))
          }
        Json.toJson(
          js.as[Map[String, JsValue]] 
            ++ Map("id" -> JsString(cmd))
        )
      }
    }
  )

}

case class DeleteColumn(
  column: Int
) extends Command
object DeleteColumn
{ implicit val format: Format[DeleteColumn] = Json.format }

//////////////////////////

case class DeleteRow(
  row: Long
) extends Command
object DeleteRow
{ implicit val format: Format[DeleteRow] = Json.format }

//////////////////////////

case class InsertColumn(
  position: Option[Int],
  name: String
) extends Command
object InsertColumn
{ implicit val format: Format[InsertColumn] = Json.format }

//////////////////////////

case class InsertRow(
  position: Long
) extends Command
object InsertRow
{ implicit val format: Format[InsertRow] = Json.format }

//////////////////////////

case class MoveColumn(
  column: Int,
  position: Int
) extends Command
object MoveColumn
{ implicit val format: Format[MoveColumn] = Json.format }

//////////////////////////

case class MoveRow(
  row: Long,
  position: Long
) extends Command
object MoveRow
{ implicit val format: Format[MoveRow] = Json.format }

//////////////////////////

case class FilteredColumn(
  columns_column: Int,
  columns_name: String
) 
{
  def column = columns_column
  def name = columns_name
}
object FilteredColumn
{ implicit val format: Format[FilteredColumn] = Json.format }

//////////////////////////

case class FilterColumns(
  columns: Seq[FilteredColumn],
) extends Command
object FilterColumns
{ implicit val format: Format[FilterColumns] = Json.format }

//////////////////////////

case class RenameColumn(
  column: Int,
  name: String
) extends Command
object RenameColumn
{ implicit val format: Format[RenameColumn] = Json.format }

//////////////////////////

case class SortColumn(
  columns_column: String,
  columns_order: String // "ASC", "DESC"
)
object SortColumn
{ implicit val format: Format[SortColumn] = Json.format }

//////////////////////////

case class UpdateCell(
  column: Int,
  row: Long,
  value: String
) extends Command
object UpdateCell
{ implicit val format: Format[UpdateCell] = Json.format }
