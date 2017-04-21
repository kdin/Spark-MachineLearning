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
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
/*import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils*/
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.util.MLUtils

object PreUpdate {
	def main(args: Array[String]) {
val conf = new SparkConf().setAppName("LoadDW")
val sc = new SparkContext(conf)
val sqlContext= new org.apache.spark.sql.SQLContext(sc)

val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()

import sqlContext._
import sqlContext.implicits._
//Read file and create RDD
val lines = sc.textFile(args(0))
val colss = lines.first().split(",")
val filtered = lines.filter(line => scala.util.Random.nextFloat < 0.2)
val parts = filtered.map(line => line.replaceAll("\\?", "0"))
//println("kkkkk"+ parts.first())
val schema = StructType(colss.map(fieldName => StructField(fieldName, DoubleType, true)))
val rowRDD = parts.map(r => Row.fromSeq(r))
var df = spark.createDataFrame(rowRDD, schema)
df = df.withColumnRenamed("Agelaius_phoeniceus", "label")
df.printSchema()

val corevar = sc.textFile("core-var")
val columns = corevar.map(l=> l.split(":"))
val feat= columns.map(token=> (token(0)))
//feat.collect().foreach(println)
val featArr = feat.collect


// classfication


val assembler = new VectorAssembler().setInputCols(featArr).setOutputCol("features")
val df3 = assembler.transform(df)
/*
val labelIndexer = new StringIndexer().setInputCol("Agelaius_phoeniceus").setOutputCol("label")
val df4 = labelIndexer.fit(df3).transform(df3)*/

val splitSeed = 5043 
val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)
val classifier = new RandomForestClassifier()
  .setImpurity("gini")
  //.setMaxDepth(3)
  .setNumTrees(300)
  .setFeatureSubsetStrategy("auto")
  .setSeed(splitSeed)
val model = classifier.fit(trainingData)
val predictions = model.transform(testData)
predictions.select("Agelaius_phoeniceus", "prediction").show(5)
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("weightedPrecision")
val accuracy = evaluator.evaluate(predictions) 
println("ACC:"+ accuracy)
}


}

