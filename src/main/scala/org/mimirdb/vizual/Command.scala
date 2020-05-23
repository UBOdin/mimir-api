package org.mimirdb.vizual

import play.api.libs.json._

sealed trait Command
object Command
{
  implicit val format = Format[Command](
    new Reads[Command]{
      def reads(j: JsValue): JsResult[Command] =
        j.as[Map[String, JsValue]]
          .get("id").get.as[String].toLowerCase match {
            case "deletecolumn"  => JsSuccess(j.as[DeleteColumn])
            case "deleterow"     => JsSuccess(j.as[DeleteRow])
            case "dropdataset"   => JsSuccess(j.as[DropDataset])
            case "insertcolumn"  => JsSuccess(j.as[InsertColumn])
            case "insertrow"     => JsSuccess(j.as[InsertRow])
            case "movecolumn"    => JsSuccess(j.as[MoveColumn])
            case "moverow"       => JsSuccess(j.as[MoveRow])
            case "projection"    => JsSuccess(j.as[FilterColumns])
            case "renamecolumn"  => JsSuccess(j.as[RenameColumn])
            case "renamedataset" => JsSuccess(j.as[RenameDataset])
            case "sortdataset"   => JsSuccess(j.as[SortDataset])
            case "updatecell"    => JsSuccess(j.as[UpdateCell])
            case _ => JsError("Not a valid Vizier command")
         }
    },
    new Writes[Command] {
      def writes(c: Command): JsValue = 
      {
        val (cmd, js) = 
          c match {
            case x:DeleteColumn  =>  ("deletecolumn",   Json.toJson(x))
            case x:DeleteRow     =>  ("deleterow",      Json.toJson(x))
            case x:DropDataset   =>  ("dropdataset",    Json.toJson(x))
            case x:InsertColumn  =>  ("insertcolumn",   Json.toJson(x))
            case x:InsertRow     =>  ("insertrow",      Json.toJson(x))
            case x:MoveColumn    =>  ("movecolumn",     Json.toJson(x))
            case x:MoveRow       =>  ("moverow",        Json.toJson(x))
            case x:FilterColumns =>  ("projection",     Json.toJson(x))
            case x:RenameColumn  =>  ("renamecolumn",   Json.toJson(x))
            case x:RenameDataset =>  ("renamedataset",  Json.toJson(x))
            case x:SortDataset   =>  ("sortdataset",    Json.toJson(x))
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
  dataset: String,
  column: String
) extends Command
object DeleteColumn
{ implicit val format: Format[DeleteColumn] = Json.format }

//////////////////////////

case class DeleteRow(
  dataset: String,
  row: Long
) extends Command
object DeleteRow
{ implicit val format: Format[DeleteRow] = Json.format }

//////////////////////////

case class DropDataset(
  dataset: String
) extends Command
object DropDataset
{ implicit val format: Format[DropDataset] = Json.format }

//////////////////////////

case class InsertColumn(
  dataset: String,
  position: Option[Int],
  name: String
) extends Command
object InsertColumn
{ implicit val format: Format[InsertColumn] = Json.format }

//////////////////////////

case class InsertRow(
  dataset: String,
  position: Long
) extends Command
object InsertRow
{ implicit val format: Format[InsertRow] = Json.format }

//////////////////////////

case class MoveColumn(
  dataset: String,
  column: String,
  position: Int
) extends Command
object MoveColumn
{ implicit val format: Format[MoveColumn] = Json.format }

//////////////////////////

case class MoveRow(
  dataset: String,
  row: Long,
  position: Long
) extends Command
object MoveRow
{ implicit val format: Format[MoveRow] = Json.format }

//////////////////////////

case class FilteredColumn(
  columns_column: String,
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
  datset: String,
  columns: Seq[FilteredColumn],
) extends Command
object FilterColumns
{ implicit val format: Format[FilterColumns] = Json.format }

//////////////////////////

case class RenameColumn(
  dataset: String,
  column: String,
  name: String
) extends Command
object RenameColumn
{ implicit val format: Format[RenameColumn] = Json.format }

//////////////////////////

case class RenameDataset(
  dataset: String,
  name: String
) extends Command
object RenameDataset
{ implicit val format: Format[RenameDataset] = Json.format }

//////////////////////////

case class SortColumn(
  columns_column: String,
  columns_order: String // "ASC", "DESC"
)
object SortColumn
{ implicit val format: Format[SortColumn] = Json.format }

//////////////////////////

case class SortDataset(
  dataset: String,
  columns: Seq[SortColumn]
) extends Command
object SortDataset
{ implicit val format: Format[SortDataset] = Json.format }

//////////////////////////

case class UpdateCell(
  dataset: String,
  column: String,
  row: Long,
  value: String
) extends Command
object UpdateCell
{ implicit val format: Format[UpdateCell] = Json.format }
