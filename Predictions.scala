package cs6240
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.mean
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.CrossValidatorModel

object Predictions {

def strippedLines(line: String) : String = {
  var elements = line.split(",")
  var one = line.split(",").slice(0,953)
  var two = line.split(",").slice(955,1016)
  var three = line.split(",").slice(1018, elements.length-1)
  // elements.slice(2,953)
  // elements.slice(955,1016)
  // elements.slice(1018,elements.length-1)
  var res = (one++two++three).mkString(",")
  
  return res
}

val corrected = functions.udf((value: Double) => value match {
  case _ => if (value >= 1) 1 else 0
  })

val correctedSamplingId = functions.udf((value: String) => value match {
  case _ => value.substring(1).toLong
  })

val toDouble = functions.udf((value: String) => value match {
  case _ => value.toDouble
  })


def main(args: Array[String]) {
val conf = new SparkConf().setAppName("LoadDW")
conf.set("spark.driver.maxResultSize", "3g")
val sc = new SparkContext(conf)
val sqlContext= new org.apache.spark.sql.SQLContext(sc)
val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
// reading core variates:


/*
val lines = sc.textFile(args(4))
val colss = lines.first()
val filtered = lines.filter(line => scala.util.Random.nextFloat < 0.005 || line.equals(colss))
val parts = filtered.map(line=>line.replaceAll("\\?","0")).map(line=>line.replaceAll(", X",",1")).map(line=>line.replaceAll(",X",",1")).map(line => strippedLines(line))
parts.saveAsTextFile("unlabled_small_2")
*/

import sqlContext._
import sqlContext.implicits._
var df = load_a(args(4),
      sqlContext).cache()


//df.printSchema()
val corevar = sc.textFile(args(2))


val cols = corevar.map(l=> l.split(":")(0))
val requiredColumns = cols.collect
df=df.withColumn("SAMPLING_EVENT_ID", correctedSamplingId(df("SAMPLING_EVENT_ID")))
requiredColumns.foreach(col=>df =df.withColumn(col, toDouble(df(col))))
 

val columns = corevar.map(l=> l.split(":"))
val feat= columns.map(token=> (token(0)))
var featArr = feat.collect

val columnNames = featArr:+"SAMPLING_EVENT_ID"
df = df.select(columnNames.head, columnNames.tail: _*)
// df.printSchema()


val assembler = new VectorAssembler().setInputCols(featArr).setOutputCol("features")
val df3 = assembler.transform(df)
                     
val model = RandomForestClassificationModel.load(args(3))
 // .setSeed(splitSeed)

val predictions = model.transform(df3)

var preds = predictions.select("SAMPLING_EVENT_ID","prediction")

var result = preds.map { case Row(samplingID:Double, value:Double) => ("S"+(samplingID.toInt.toString()), value.toInt)}
var ress = result.select("_1", "_2")
ress=ress.withColumnRenamed("_1", "SAMPLING_EVENT_ID")
ress=ress.withColumnRenamed("_2", "SAW_AGELAIUS_PHOENICEUS")
ress.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("Result1")
}


 def load_a(path: String, sqlContext: SQLContext): DataFrame = {
    var data = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    return data
  }
}
