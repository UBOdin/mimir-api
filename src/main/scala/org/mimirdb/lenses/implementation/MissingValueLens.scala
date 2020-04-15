package org.mimirdb.lenses.implementation

import play.api.libs.json._
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.mimirdb.caveats.implicits._
import org.mimirdb.lenses.Lens
import org.mimirdb.spark.SparkPrimitive.dataTypeFormat
import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.Column
import org.mimirdb.rowids.AnnotateWithRowIds
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.classification.OneVsRest
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.feature.ImputerModel
import org.apache.spark.ml.Transformer
import java.util.UUID
import java.io.File
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.Dataset
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}

case class MissingValueLensConfig(
  colsStrategy: Seq[MissingValueImputerConfig]
)

object MissingValueLensConfig
{
  implicit val format:Format[MissingValueLensConfig] = Json.format

  def apply(cols: Seq[String], df: DataFrame): MissingValueLensConfig =
  {
    val modelUuid = UUID.randomUUID().toString()
    val colsStrategy = cols.map(mvcol => {
      val t = df.schema(mvcol).dataType
      val modelPath = s"${modelUuid}-${mvcol}.model"
      t match {
        case nt:NumericType => MissingValueImputerConfig(classOf[MeanMedianImputer].getSimpleName , mvcol, "mean", modelPath)
        case BooleanType => MissingValueImputerConfig(classOf[MulticlassImputer].getSimpleName, mvcol, "GradientBoostedTreeBinary", modelPath)
        case x => MissingValueImputerConfig(classOf[MulticlassImputer].getSimpleName, mvcol, "NaiveBayes", modelPath)
      }
      
    })

    MissingValueLensConfig(
      colsStrategy:Seq[MissingValueImputerConfig]
    )
  }
  
}



object MissingValueLens
  extends Lens
{
  def train(input: DataFrame, rawConfig: JsValue): JsValue = 
  {
    val config = 
      rawConfig match {
        case JsArray(cols) => {
          MissingValueLensConfig(cols.map(_.as[String]), input)
        }
        case _:JsObject => rawConfig.as[MissingValueLensConfig]
        case _ => throw new IllegalArgumentException(s"Invalid MissingValueLens configuration: $rawConfig")
      }
    val fieldNames = input.schema.fieldNames
    config.colsStrategy.foreach( imputerConfig => {
      //logger.debug(s"creating imputer model for: ${imputerConfig.productIterator.mkString(",")}")
      imputerConfig.imputer.model(input)
    })
    Json.toJson(
      config
    )
  }
  def create(input: DataFrame, rawConfig: JsValue, context: String): DataFrame = 
  {
    val config = rawConfig.as[MissingValueLensConfig]
    val fieldNames = input.schema.fieldNames
    val completedf = config.colsStrategy.foldLeft(input)((inputdf, imputerConfig) => {
      val imputeCol = imputerConfig.imputeCol
      val schemaRev = input.schema(imputeCol)
      val fieldRef = inputdf(imputeCol)
      val caveatedDf = inputdf.select(fieldNames.map(ccol => 
        if(ccol.equalsIgnoreCase(imputeCol)){ 
          val frCond = schemaRev.dataType match { 
            case x:NumericType => fieldRef.isNull.or(fieldRef.isNaN)
            case _ => fieldRef.isNull
          }
          fieldRef.caveatIf(s"$imputeCol was null and we imputed it with ${imputerConfig.strategy}", 
             frCond ).as(imputeCol) 
        } else {
          col(ccol)
        }):_*)
      val outdf = imputerConfig.imputer.impute(caveatedDf);
      //println(s"created mv lens col df: ${imputerConfig.productIterator.mkString(",")}")
      outdf
    }).select(fieldNames.map(col(_)):_*)
    //completedf.show(20)
    completedf
  }

}

case class MissingValueImputerConfig(modelType:String, imputeCol:String, strategy:String, modelPath:String){
  def imputer:MissingValueImputer = {
    modelType match {
      case "MeanMedianImputer" => MeanMedianImputer(imputeCol, strategy, modelPath)
      case "MulticlassImputer" => MulticlassImputer(imputeCol, strategy, modelPath)
      case x => throw new Exception(s"unknown model type: $x")
    }
  }
}
object MissingValueImputerConfig
{
  implicit val format:Format[MissingValueImputerConfig] = Json.format
}

sealed trait MissingValueImputer {
  def impute(input:DataFrame) : DataFrame
  def model(input:DataFrame) : Transformer
}

case class MeanMedianImputer(imputeCol:String, strategy:String, modelPath:String) extends MissingValueImputer{
  def impute(input:DataFrame) : DataFrame = {
      model(input).transform(input)
  }
  def model(input:DataFrame):Transformer = {
    val modelFile = new File(modelPath)
    if(modelFile.exists()) 
      ImputerModel.load(modelPath)
    else {
      val imputer = new Imputer().
      setStrategy(strategy).
      setMissingValue(0).
      setInputCols(Array(imputeCol)).setOutputCols(Array(imputeCol));
      val fieldRef = input(imputeCol)
      val imputerModel = imputer.fit(input.filter(fieldRef.isNotNull))
      imputerModel.save(modelPath)
      imputerModel
    }
  }
}

case class MulticlassImputer(imputeCol:String, strategy:String, modelPath:String) extends MissingValueImputer{
  def impute(input:DataFrame) : DataFrame = {
    //println(s"imputing mv: ${productIterator.mkString(",")}----------------")
    input.show(20)
    val imputeddf = model(input).transform(input)
    //println(s"imputed mv: ${productIterator.mkString(",")}-----------------")
    //imputeddf.show(20)
    imputeddf
  }
  def model(input:DataFrame):Transformer = {
    val modelFile = new File(modelPath)
    if(modelFile.exists()){ 
      PipelineModel.load(modelPath)
    }
    else {
      val imputerModel = MulticlassImputer.classifierPipelines(strategy)(input, MulticlassImputerParams(imputeCol))
      imputerModel.save(modelPath)
      imputerModel
    }
  }
}

object MulticlassImputerParamsHandleInvalid extends Enumeration {
  val keep, skip, error = Value
}

case class MulticlassImputerParams(predictionCol:String, handleInvalid:MulticlassImputerParamsHandleInvalid.Value=MulticlassImputerParamsHandleInvalid.keep)

object MulticlassImputer {
  val PREDICTED_LABEL_COL = "predictedLabel"
  private def extractFeatures(training:DataFrame, params:MulticlassImputerParams):(Array[String], Seq[PipelineStage]) = {
      val cols = training.schema.fields
      //training.show()
      val featCols = cols.filterNot(_.name.equals(params.predictionCol))
      val trainingIndexable = training.withColumn(params.predictionCol, training(params.predictionCol).cast(StringType))
      val stringIndexCaster = new CastForStringIndex().setInputCol(params.predictionCol).setOutputCol(params.predictionCol)
      val indexer = new StringIndexer().setInputCol(params.predictionCol).setOutputCol("label").setHandleInvalid(params.handleInvalid.toString())
      val labels = indexer.fit(trainingIndexable).labels
      val (nullReplacers, tokenizers, hashingTFs) = featCols.flatMap(col => {
        col.dataType match {
          case StringType => {
            val nullReplacer = new ReplaceNullsForCollumn().setReplaceColumn(col.name).setReplacementColumn("0")
            val tokenizer = new RegexTokenizer().setInputCol(col.name).setOutputCol(s"${col.name}_words")
            val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol(s"${col.name}_features").setNumFeatures(20)
            Some((nullReplacer, tokenizer, hashingTF))
          }
          case _ => None
        }
      }).unzip3
      val assmblerCols = featCols.flatMap(col => {
        col.dataType match {
          case StringType => Some(s"${col.name}_features")
          case x:NumericType => Some(col.name)
          case _ => None
        }
      })
      val assembler = new VectorAssembler().setInputCols(assmblerCols.toArray).setOutputCol("rawFeatures")
      val normlzr = new Normalizer().setInputCol("rawFeatures").setOutputCol("normFeatures").setP(1.0)
      val scaler = new StandardScaler().setInputCol("normFeatures").setOutputCol("features").setWithStd(true).setWithMean(false)
      //println("---")
      //println(tokenizers.toList)
      //println("---")
      //println(hashingTFs.toList)
      (labels,(stringIndexCaster :: new StagePrinter("indexcast") :: indexer :: new StagePrinter("indexer") :: nullReplacers ++: tokenizers ++: hashingTFs ++: (new StagePrinter("tokhash") :: assembler :: new StagePrinter("assembler") :: normlzr :: new StagePrinter("norm") :: scaler :: Nil)))
    }

    val classifierPipelines = Map[String, (DataFrame, MulticlassImputerParams) => PipelineModel](
      ("NaiveBayes", (trainingData, params) => {
        import org.apache.spark.sql.functions.abs
        val trainingp = trainingData.na.drop//.withColumn(params.predictionCol, trainingData(params.predictionCol).cast(StringType))
        val training = trainingp.schema.fields.filter(col => col.dataType match {
          case x:NumericType => true
          case _ => false
        } ).foldLeft(trainingp)((init, cur) => init.withColumn(cur.name,abs(init(cur.name))) )
        val (labels, featurePipelineStages) = extractFeatures(training,params)
        val classifier = new NaiveBayes().setLabelCol("label").setFeaturesCol("features")//.setModelType("multinomial")
        val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol(PREDICTED_LABEL_COL).setLabels(labels)
        val replaceNulls = new ReplaceNullsForCollumn().setReplaceColumn(params.predictionCol).setReplacementColumn(PREDICTED_LABEL_COL)
        val stages = featurePipelineStages ++: (new StagePrinter("features") :: classifier :: new StagePrinter("classifier") :: labelConverter :: new StagePrinter("labelconvert") :: replaceNulls :: Nil)
        val pipeline = new Pipeline().setStages(stages.toArray)
        pipeline.fit(training.withColumn(params.predictionCol, training(params.predictionCol).cast(StringType)))
      }),
   
      ("RandomForest", (trainingData, params) => {
        val training = trainingData.na.drop
        val (labels, featurePipelineStages) = extractFeatures(training,params)
        val classifier = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features")
        val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol(PREDICTED_LABEL_COL).setLabels(labels)
        val stages = featurePipelineStages ++: (classifier :: labelConverter :: Nil)
        val pipeline = new Pipeline().setStages(stages.toArray)
        pipeline.fit(training)
      }),
  
      ("DecisionTree", (trainingData, params) => {
        val training = trainingData.na.drop
        val (labels, featurePipelineStages) = extractFeatures(training,params)
        val classifier = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")
        val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol(PREDICTED_LABEL_COL).setLabels(labels)
        val stages = featurePipelineStages ++: ( classifier :: labelConverter :: Nil)
        val pipeline = new Pipeline().setStages(stages.toArray)
        pipeline.fit(training)
      }),
  
      ("GradientBoostedTreeBinary", (trainingData, params) => {
        val training = trainingData.na.drop
        val (labels, featurePipelineStages) = extractFeatures(training,params)
        val featureIndexer = new VectorIndexer().setInputCol("assembledFeatures").setOutputCol("features").setMaxCategories(20)
        val classifier = new GBTClassifier().setLabelCol("label").setFeaturesCol("features").setMaxIter(10)
        val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol(PREDICTED_LABEL_COL).setLabels(labels)
        val stages = featurePipelineStages ++: ( featureIndexer :: classifier :: labelConverter :: Nil)
        val pipeline = new Pipeline().setStages(stages.toArray)
        pipeline.fit(training.withColumn(params.predictionCol, training(params.predictionCol).cast(StringType)))
      }),
      
      ("LogisticRegression", (trainingData, params) => {
        val training = trainingData.na.drop
        val (labels, featurePipelineStages) = extractFeatures(training,params)
        val classifier = new LogisticRegression().setMaxIter(10).setTol(1E-6).setFitIntercept(true).setLabelCol("label").setFeaturesCol("features")
        val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol(PREDICTED_LABEL_COL).setLabels(labels)
        val stages = featurePipelineStages ++: ( classifier :: labelConverter :: Nil)
        val pipeline = new Pipeline().setStages(stages.toArray)
        pipeline.fit(training)
      }),
      
      ("OneVsRest", (trainingData, params) => {
        val training = trainingData.na.drop
        val (labels, featurePipelineStages) = extractFeatures(training,params)
        val classifier = new LogisticRegression().setMaxIter(10).setTol(1E-6).setFitIntercept(true).setLabelCol("label").setFeaturesCol("features")
        val ovr = new OneVsRest().setClassifier(classifier)
        val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol(PREDICTED_LABEL_COL).setLabels(labels)
        val stages = featurePipelineStages ++: ( ovr :: labelConverter :: Nil)
        val pipeline = new Pipeline().setStages(stages.toArray)
        pipeline.fit(training)
      }),
      
      ("LinearSupportVectorMachineBinary", (trainingData, params) => {
        val training = trainingData.na.drop
        val (labels, featurePipelineStages) = extractFeatures(training,params)
        val classifier = new LinearSVC().setMaxIter(10).setRegParam(0.1).setLabelCol("label").setFeaturesCol("features")
        val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol(PREDICTED_LABEL_COL).setLabels(labels)
        val stages = featurePipelineStages ++: ( classifier :: labelConverter :: Nil)
        val pipeline = new Pipeline().setStages(stages.toArray)
        pipeline.fit(training)
      }),
      
      ("MultilayerPerceptron", (trainingData, params) => {
        val training = trainingData.na.drop
        val (labels, featurePipelineStages) = extractFeatures(training,params)
        import org.apache.spark.sql.functions.countDistinct
        import org.apache.spark.sql.functions.col
        val classCount = training.select(countDistinct(col(params.predictionCol))).head.getLong(0)
        val layers = Array[Int](training.columns.length, 8, 4, classCount.toInt)
        val classifier = new MultilayerPerceptronClassifier()
          .setLayers(layers).setBlockSize(128).setSeed(1234L).setMaxIter(100).setLabelCol("label").setFeaturesCol("features")
        val labelConverter = new IndexToString().setInputCol(classifier.getPredictionCol).setOutputCol(PREDICTED_LABEL_COL).setLabels(labels)
        val stages = featurePipelineStages ++: ( classifier :: labelConverter :: Nil)
        val pipeline = new Pipeline().setStages(stages.toArray)
        pipeline.fit(training)
      }))
}

class CastForStringIndex(override val uid: String) 
  extends Transformer 
  with DefaultParamsWritable {
  final val inputCol= new Param[String](this, "inputCol", "The input column")
  final val outputCol = new Param[String](this, "outputCol", "The output column")
  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)
  def this() = this(Identifiable.randomUID("castforstringindex"))
  def copy(extra: ParamMap): CastForStringIndex = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
  def transform(df: Dataset[_]): DataFrame = df.withColumn($(outputCol), df($(inputCol)).cast(StringType))
}
  
object CastForStringIndex extends DefaultParamsReadable[CastForStringIndex] {
  override def load(path: String): CastForStringIndex = super.load(path)
}

class ReplaceNullsForCollumn(override val uid: String) 
  extends Transformer 
  with DefaultParamsWritable {
  final val replaceColumn= new Param[String](this, "replaceColumn", "The column to replace")
  final val replacementColumn= new Param[String](this, "replacementColumn", "The column to replace with")
  def setReplaceColumn(value: String): this.type = set(replaceColumn, value)
  def setReplacementColumn(value: String): this.type = set(replacementColumn, value)
  def this() = this(Identifiable.randomUID("replacenullsforcollumn"))
  def copy(extra: ParamMap): ReplaceNullsForCollumn = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
  def transform(df: Dataset[_]): DataFrame = df.withColumn($(replaceColumn), when(df($(replaceColumn)).isNull, expr($(replacementColumn))).otherwise(df($(replaceColumn)))  )
}
  
object ReplaceNullsForCollumn extends DefaultParamsReadable[ReplaceNullsForCollumn] {
  override def load(path: String): ReplaceNullsForCollumn = super.load(path)
}

class StagePrinter(val sname:String, override val uid: String) 
  extends Transformer 
  with DefaultParamsWritable {
  final val stageName = new Param[String](this, "stageName", "The stage name to print")
  def this(stageName:String) = {
    this(stageName, Identifiable.randomUID("stageprinter"))
    this.setStageName(stageName)
  }
  def this() = this("", Identifiable.randomUID("stageprinter"))
  def setStageName(value: String): this.type = set(stageName, value)
  def copy(extra: ParamMap): StagePrinter = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = schema
  def transform(df: Dataset[_]): DataFrame = {
    ////println(s"------------------pipeline stage: ${$(stageName)} dataframe")
    //df.show()
    df.select(col("*"))
  }
}
  
object StagePrinter extends DefaultParamsReadable[StagePrinter] {
  override def load(path: String): StagePrinter = super.load(path)
}