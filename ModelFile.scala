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
  
  return res
}


/*def getAccuracy(arg: (Double, Double)) = {
  if (arg._1 == arg._2) {
    accum.add(1)
  }
  total.add(1)
}

*/


val corrected = functions.udf((value: Double) => value match {
  case _ => if (value >= 1) 1 else 0
  })

val toDouble = functions.udf((value: String) => value match {
  case _ => value.toDouble
  })

def main(args: Array[String]) {
val conf = new SparkConf().setAppName("LoadDW")
conf.set("spark.driver.maxResultSize", "3g")
val sc = new SparkContext(conf)
val sqlContext= new org.apache.spark.sql.SQLContext(sc)
val accum = sc.longAccumulator("Accuracy")
val total = sc.longAccumulator("Total")

val spark = SparkSession
  .builder()
  .appName("Spark SQL basic example")
  .config("spark.some.config.option", "some-value")
  .getOrCreate()
// reading core variates:




/*val lines = sc.textFile(args(0))
val colss = lines.first()
val filtered = lines.filter(line => scala.util.Random.nextFloat < 0.005 || line.equals(colss))
val parts = filtered.map(line=>line.replaceAll("\\?","0")).map(line=>line.replaceAll(", X",",1")).map(line=>line.replaceAll(",X",",1")).map(line => strippedLines(line))
parts.saveAsTextFile(args(1))

*/

import sqlContext._
import sqlContext.implicits._
var df = load_a(args(1),
      sqlContext).cache()


//df.printSchema()

val corevar = sc.textFile(args(2))



val cols = corevar.map(l=> l.split(":")(0))
cols.collect.foreach(col=>df =df.withColumn(col, toDouble(df(col))))
df=df.withColumn("Agelaius_phoeniceus", corrected(df("Agelaius_phoeniceus")))


val fields = df.schema.fields filter {
x => x.dataType match { 
      case x: org.apache.spark.sql.types.StringType => true
      case _ => false 
      } 
    } map { x => x.name }

val newDf = fields.foldLeft(df){ case(dframe,field) => dframe.drop(field) }

df = newDf

// df.printSchema()    

val columns = corevar.map(l=> l.split(":"))
val feat= columns.map(token=> (token(0)))
var featArr = feat.collect

val columnNames = featArr
df = df.select(columnNames.head, columnNames.tail: _*)
// df.printSchema()

featArr = featArr.filter(line => !line.equals("Agelaius_phoeniceus"))



val assembler = new VectorAssembler().setInputCols(featArr).setOutputCol("features")
val df3 = assembler.transform(df)
// df3.printSchema()
val labelIndexer = new StringIndexer().setInputCol("Agelaius_phoeniceus").setOutputCol("label")
val df4 = labelIndexer.fit(df3).transform(df3)

val splitSeed = 679
val Array(trainingData, testData) = df4.randomSplit(Array(0.7, 0.3))




/*
val layers = Array[Int](66, 60, 55, 52, 47, 42, 37, 32, 27, 22, 17, 12, 7, 4, 2)

// create the trainer and set its parameters
val trainer = new MultilayerPerceptronClassifier()
  .setLayers(layers)
  .setTol(1E-10)
  .setMaxIter(400)
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setBlockSize(128)
  .setSeed(1234L)

// train the model
val model = trainer.fit(trainingData)*/

/*
val gbt = new GBTClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setMaxIter(10)

val model = gbt.fit(trainingData)*/


/*
val logisticRegressionClassifier = new LogisticRegression()
                                      .setLabelCol("label")
                                      .setFeaturesCol("features")
                                      .setMaxIter(300)
                                      .setTol(1E-10)
val model = logisticRegressionClassifier.fit(trainingData)    */                                    


val classifier = new RandomForestClassifier()
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setImpurity("gini")
  .setMaxBins(100)
  .setNumTrees(30)
  .setFeatureSubsetStrategy("auto")
 // .setSeed(splitSeed)

val paramGrid = new ParamGridBuilder()
                    .addGrid(classifier.maxDepth, Array(10,15,20,30))
                    .build()
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  // "f1" (default), "weightedPrecision", "weightedRecall", "accuracy"
  .setMetricName("accuracy") 

val pipeline = new Pipeline().setStages(Array(classifier))
val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(4)

val model = cv.fit(trainingData)


val predictions = model.transform(testData)

val preds = predictions.select("label", "prediction")


val labelAndPreds = preds.map { case Row(label:Double, prediction:Double) => (label, prediction)}
val accr = labelAndPreds.filter(r => r._1 == r._2).count.toDouble/testData.count()

println("ACCURACY ::::::: ", accr)
/*val evaluator = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("prediction")
  // .setMetricName("accuracy")

val accuracy = evaluator.evaluate(predictions) 
println("ACC:"+ accuracy)
*/}


 def load_a(path: String, sqlContext: SQLContext): DataFrame = {
    var data = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    return data
  }
}
