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

/*
import sqlContext._
import sqlContext.implicits._*/
//Read file and create RDD
 /*val lines = sc.textFile(args(0))
 val parts = lines.map(line => line.replaceAll("\\?", ""))

val schema = StructType(featArr.map(fieldName => StructField(fieldName, DoubleType, true)))
val rowRDD = parts.map(r => Row.fromSeq(r))


var df = spark.createDataFrame(rowRDD, schema)
df.printSchema()
df.show*/


/*val extendedCore = sc.textFile("extended-core")
val extendedColumns = extendedCore.map(l=> l.split(":"))
val extendedFeat= extendedColumns.map(token=> (token(0)))
val extFeatArr = extendedFeat.collect*/

val lines = sc.textFile(args(0))
val colss = lines.first()
val filtered = lines.filter(line => scala.util.Random.nextFloat < 0.005 || line.equals(colss))
val parts = filtered.map(line => line.replaceAll("\\?", "0"))
parts.coalesce(1).saveAsTextFile("OutputN")

import sqlContext._
import sqlContext.implicits._
var df = load_a("OutputN",
      sqlContext).cache()

//df.select(df.columns.map(c => c.replaceAll("\\?", "")))

/*
val colsToRemove = Seq("SAMPLING_EVENT_ID0","LOC_ID1",  "SAMPLING_EVENT_ID1016", "LOC_ID1017", "SAMPLING_EVENT_ID953", "LOC_ID954") 

val filteredDF = df.select(df.columns .filter(colName => !colsToRemove.contains(colName)) .map(colName => new Column(colName)): _*)
//filteredDF.printSchema()
val newDf = filteredDF.withColumnRenamed("SAMPLING_EVENT_ID0", "SAMPLING_EVENT_ID")
val actualDf = newDf.withColumnRenamed("LOC_ID1", "LOC_ID")

//actualDf.printSchema()
println("array:"+ featArr.mkString(","))
val selectedData = filteredDF.select(featArr.head, featArr.tail: _*)
selectedData.printSchema()*/


/*val colType  = df.dtypes
colType.foreach(c=>println(c))*/
//df = df.collect()
/*colType.foreach(l=> imputed(l))*//*
def imputed(col: (String,String)) = {
  val mean1 = df.select(mean(col._1))*/
 // val meeaann = mean1.rdd
 // val mmmmmm= mean1.first()
  //println("here::"+ mmmmmm.collect())
  //println("mean:::"+mean1)
 // val mean1 = df.select(mean(col._1)).first()(0).asInstanceOf[Double]
 // println("mean:::"+mean1)
  /*if (col._2.equals("IntegerType")) {
    if (mean1 >= 0.5) 
      df = df.na.fill(1, Seq(col._1))
    else
      df = df.na.fill(0, Seq(col._1)) 
  }
  else if (col._2.equals("DoubleType")) {
    df = df.na.fill(mean1, Seq(col._1))   
  }*/
  //}







val assembler = new VectorAssembler().setInputCols(featArr).setOutputCol("features")
val df3 = assembler.transform(df)

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