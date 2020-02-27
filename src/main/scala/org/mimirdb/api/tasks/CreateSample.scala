package org.mimirdb.api.tasks

import play.api.libs.json._
import org.apache.spark.sql.catalyst.plans.logical.{ LogicalPlan, Filter } 
import org.apache.spark.sql.functions.{ rand, udf, col }
import org.apache.spark.sql.types.DataType
import org.mimirdb.api.SparkPrimitive

object CreateSample
{

  def apply(
    /* query string to get schema for - table name */
      sourceTable: String,
    /* target table to define as the sampling view */
      targetTable: String,
    /* mode configuration */
      samplingMode: SamplingMode,
    /* seed - optional long */
      seed: Option[Long],
  ){
    ???
  }

  trait SamplingMode
  {
    def apply(plan: LogicalPlan, seed: Long): LogicalPlan
    def toJson: JsValue
  }

  val parsers = Seq[Map[String,JsValue] => Option[SamplingMode]](
    SampleStratifiedOn.parseJson,
    SampleRowsUniformly.parseJson
  )

  def modeFromJson(v: JsValue): SamplingMode =
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

  implicit val modeFormat = Format[SamplingMode](
    new Reads[SamplingMode]{ def reads(v: JsValue) = JsSuccess(modeFromJson(v)) },
    new Writes[SamplingMode]{ def writes(mode: SamplingMode) = mode.toJson }
  )


  case class SampleRowsUniformly(probability:Double) extends SamplingMode
  {
    override def toString = s"WITH PROBABILITY $probability"

    def apply(plan: LogicalPlan, seed: Long): LogicalPlan = 
    {
      // Adapted from Spark's df.stat.sampleBy method
      val r = rand(seed)
      val f = udf { (x: Double) => x < probability }
      Filter(
        f(r).expr,
        plan
      )
    }


    def toJson: JsValue = JsObject(Map[String,JsValue](
      "mode" -> JsString(SampleRowsUniformly.MODE),
      "probability" -> JsNumber(probability)
    ))
  }

  object SampleRowsUniformly
  {
    val MODE = "uniform_probability"

    def parseJson(json:Map[String, JsValue]): Option[SampleRowsUniformly] =
    {
      if(json("mode").as[String].equals(MODE)){
        Some(SampleRowsUniformly(json("probability").as[Double]))
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
  case class SampleStratifiedOn(column:String, t: DataType, strata:Map[Any,Double]) extends SamplingMode
  {
    override def toString = s"ON $column WITH STRATA ${strata.map { case (v,p) => s"$v -> $p"}.mkString(" | ")}"


    def apply(plan: LogicalPlan, seed: Long): LogicalPlan =
    {
      // Adapted from Spark's df.stat.sampleBy method
      val c = col(column)
      val r = rand(seed)
      val f = udf { (stratum: Any, x: Double) =>
                x < strata.getOrElse(stratum, 0.0)
              }
      Filter(
        f(c, r).expr,
        plan
      )
    }

    def toJson: JsValue = JsObject(Map[String,JsValue](
      "mode" -> JsString(SampleStratifiedOn.MODE),
      "column" -> JsString(column),
      "strata" -> JsArray(
        strata
          .toSeq
          .map { case (v, p) => JsObject(Map[String,JsValue](
              "value" -> SparkPrimitive.encode(v, t),
              "probability" -> JsNumber(p)
            ))
          }
      )
    ))
  }

  object SampleStratifiedOn
  {
    val MODE = "stratified_on"

    def parseJson(json:Map[String, JsValue]): Option[SampleStratifiedOn] =
    {
      if(json("mode").as[String].equals(MODE)){
        val t = DataType.fromJson(json("type").toString)
        Some(SampleStratifiedOn(
          json("column").as[String],
          t,
          json("strata")
            .as[Seq[Map[String,JsValue]]]
            .map { stratum => 
              SparkPrimitive.decode(stratum("value"), t) -> 
                stratum("probability").as[Double]
            }
            .toMap
        ))
      } else {
        None
      }
    }
  }
}

