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

object ModelFile {

def strippedLines(line: String) : String = {
  var elements = line.split(",")
  var one = line.split(",").slice(2,953)
  var two = line.split(",").slice(955,1016)
  var three = line.split(",").slice(1018, elements.length-1)
  // elements.slice(2,953)
  // elements.slice(955,1016)
  // elements.slice(1018,elements.length-1)
  var res = (one++two++three).mkString(",")
  res.replaceAll("\\?", "0")
  return res
}


val toDouble = functions.udf((value: String) => value match {
  case _ => value.toDouble
  })

	def main(args: Array[String]) {
val conf = new SparkConf().setAppName("LoadDW")
val sc = new SparkContext(conf)
val sqlContext= new org.apache.spark.sql.SQLContext(sc)


val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
// reading core variates:

val corevar = sc.textFile("core-var")
val columns = corevar.map(l=> l.split(":"))
val feat= columns.map(token=> (token(0)))
feat.collect().foreach(println)
val featArr = feat.collect


val lines = sc.textFile(args(0))
val colss = lines.first()
val filtered = lines.filter(line => scala.util.Random.nextFloat < 0.005 || line.equals(colss))
filtered.collect
val parts = filtered.map(line => strippedLines(line))
parts.coalesce(1).saveAsTextFile(args(1))


import sqlContext._
import sqlContext.implicits._
var df = load_a(args(1),
      sqlContext).cache()

//df.printSchema()
df =df.withColumn("ELEV_GT", toDouble(df("ELEV_GT")))
df.printSchema()    


val assembler = new VectorAssembler().setInputCols(featArr).setOutputCol("features")
val df3 = assembler.transform(df)
df3.printSchema()
val labelIndexer = new StringIndexer().setInputCol("Agelaius_phoeniceus").setOutputCol("label")
val df4 = labelIndexer.fit(df3).transform(df3)

val splitSeed = 5043 
val Array(trainingData, testData) = df4.randomSplit(Array(0.7, 0.3), splitSeed)
val classifier = new RandomForestClassifier()
  .setImpurity("gini")
  //.setMaxDepth(3)
  .setNumTrees(10)
  .setFeatureSubsetStrategy("auto")
  .setSeed(splitSeed)
val model = classifier.fit(trainingData)
val predictions = model.transform(testData)
predictions.select("Agelaius_phoeniceus", "label", "prediction").show(5)
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("weightedPrecision")
val accuracy = evaluator.evaluate(predictions) 
println("ACC:"+ accuracy)
}


 def load_a(path: String, sqlContext: SQLContext): DataFrame = {
    var data = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
      //.toDF(featuresArr: _*)
    return data
  }
}
