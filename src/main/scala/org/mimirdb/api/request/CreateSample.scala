package org.mimirdb.api.request

import java.sql.SQLException
import scala.util.Random
import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, Filter } 
import org.apache.spark.sql.functions.{ rand, udf, col }
import org.apache.spark.sql.types.DataType
import org.mimirdb.spark.SparkPrimitive
import org.mimirdb.api.{ Request, Response, MimirAPI }
import org.mimirdb.data.{ DataFrameConstructor, DataFrameConstructorCodec }


object Sample
{

  trait Mode
  {
    def apply(plan: DataFrame, seed: Long): DataFrame
    def toJson: JsValue
  }

  val parsers = Seq[Map[String,JsValue] => Option[Mode]](
    StratifiedOn.parseJson,
    Uniform.parseJson
  )

  def modeFromJson(v: JsValue): Mode =
  {
    val config = v.as[Map[String,JsValue]]
    for(parser <- parsers){
      parser(config) match {
        case Some(s) => return s
        case None => ()
      }
    }
    throw new RuntimeException(s"Invalid Sampling Mode: $v")
  }

  implicit val modeFormat = Format[Mode](
    new Reads[Mode]{ def reads(v: JsValue) = JsSuccess(modeFromJson(v)) },
    new Writes[Mode]{ def writes(mode: Mode) = mode.toJson }
  )


  case class Uniform(probability:Double) extends Mode
  {
    override def toString = s"WITH PROBABILITY $probability"

    def apply(df: DataFrame, seed: Long): DataFrame = 
      df.sample(probability, seed)


    def toJson: JsValue = JsObject(Map[String,JsValue](
      "mode" -> JsString(Uniform.MODE),
      "probability" -> JsNumber(probability)
    ))
  }

  object Uniform
  {
    val MODE = "uniform_probability"

    def parseJson(json:Map[String, JsValue]): Option[Uniform] =
    {
      if(json("mode").as[String].equals(MODE)){
        Some(Uniform(json("probability").as[Double]))
      } else {
        None
      }
    }
  }

  /** 
   * Generate a sample of the dataset stratified on the specified column
   * 
   * A stratified sample allows sampling with variable rates depending on 
   * the value of the specified column.  Its most frequent use is to ensure
   * fairness between strata, regardless of their distribution in the 
   * original dataset.  For example, this could be used to derive a sample
   * of demographic data with equal representations from all ethnicities, 
   * even if one ethnicity is under-represented.
   * 
   * Sampling rate is given as a probability.  The final sample will 
   * contain approximately `strata(value) * count(df.col = value)` records
   * where `df.col = value`.  
   *
   * @param    column    The column to use to determine the sampling rate
   * @param    strata    A map from a value for [[column]] to the probability of 
   *                     sampling the value. Non-specified values will not be 
   *                     included in the sample.
   **/
  case class StratifiedOn(column:String, strata:Seq[(JsValue,Double)]) extends Mode
  {
    override def toString = s"ON $column WITH STRATA ${strata.map { case (v,p) => s"$v -> $p"}.mkString(" | ")}"

    def apply(df: DataFrame, seed: Long): DataFrame = 
    {
      val t = df.schema.fields.find { _.name.equals(column) }.get.dataType
      df.stat.sampleBy(
        column, 
        strata.map { stratum => 
          SparkPrimitive.decode(stratum._1, t) -> stratum._2 
        }.toMap, 
        seed
      )
    }

    def toJson: JsValue = Json.obj(
      "mode" -> JsString(StratifiedOn.MODE),
      "column" -> JsString(column),
      "strata" -> JsArray(
        strata
          .map { case (v, p) => Json.obj(
              "value" -> v,
              "probability" -> JsNumber(p)
            )
          }
      )
    )
  }

  object StratifiedOn
  {
    val MODE = "stratified_on"

    def parseJson(json:Map[String, JsValue]): Option[StratifiedOn] =
    {
      if(json("mode").as[String].equals(MODE)){
        Some(StratifiedOn(
          json("column").as[String],
          json("strata")
            .as[Seq[Map[String,JsValue]]]
            .map { stratum => 
              stratum("value") -> stratum("probability").as[Double]
            }
        ))
      } else {
        None
      }
    }
  }
}




case class CreateSampleRequest (
            /* query string to get schema for - table name */
                  source: String,
            /* mode configuration */
                  samplingMode: Sample.Mode,
            /* seed - optional long */
                  seed: Option[Long],
            /* optional name for the result table */
                  resultName: Option[String],
            /* optional properties */
                  properties: Option[Map[String,JsValue]]
) extends Request with DataFrameConstructor {

  def construct(spark: SparkSession, context: Map[String,DataFrame]): DataFrame =
    samplingMode.apply(context(source), seed.getOrElse { new Random().nextLong })

  def handle = {
    if(!MimirAPI.catalog.exists(source)){
      throw new SQLException("Table $source does not exist")
    }
    val output = 
      resultName.getOrElse {
        s"SAMPLE_${(source+samplingMode.toString+seed.toString).hashCode().toString().replace("-", "")}"
      }
    MimirAPI.catalog.put(output, this, Set(source), properties = properties.getOrElse { Map.empty })
    Json.toJson(CreateSampleResponse(output))
  }
}

object CreateSampleRequest extends DataFrameConstructorCodec {
  implicit val format: Format[CreateSampleRequest] = Json.format
  def apply(j: JsValue) = j.as[CreateSampleRequest]
}

case class CreateSampleResponse (
            /* name of resulting view */
                  viewName: String
) extends Response

object CreateSampleResponse {
  implicit val format: Format[CreateSampleResponse] = Json.format
}

