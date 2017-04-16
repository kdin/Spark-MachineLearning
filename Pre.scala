package cs6240
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
//import org.apache.spark.sql.SQLContext.implicits
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.mean
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.SQLContext.implicits
//import org.apache.spark.sql.SparkSession


object Pre {
	def main(args: Array[String]) {
val conf = new SparkConf().setAppName("LoadDW")
val sc = new SparkContext(conf)
val sqlContext= new org.apache.spark.sql.SQLContext(sc)
//Read file and create RDD
val lines = sc.textFile(args(0))
val parts = lines.map(line => line.replaceAll("\\?", ""))
//val df = parts.toDf()
 parts.coalesce(1).saveAsTextFile("Output")

import sqlContext._
import sqlContext.implicits._
var df = load_a("Output/",
      sqlContext,
      "PassengerId", "Survived", "Pclass", "Name", "Sex", "Age", "SibSp", "Parch", "Ticket", "Fare", "Cabin", "Embarked").cache()
df.printSchema()
//val a0 = df.collect.map(att=> att(0)).dataType
val colType  = df.dtypes
colType.foreach(l=> imputed(l))
def imputed(col: (String,String)) = {
	val mean1 = df.select(mean(col._1)).first()(0).asInstanceOf[Double]
	if (col._2.equals("IntegerType")) {
		if (mean1 >= 0.5) 
			df = df.na.fill(1, Seq(col._1))
		else
			df = df.na.fill(0, Seq(col._1))	
	}
	else if (col._2.equals("DoubleType")) {
		df = df.na.fill(mean1, Seq(col._1))		
	}
	}
		
}

df.printSchema()
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save("Output1")
}




 def load_a(path: String, sqlContext: SQLContext, featuresArr: String*): DataFrame = {
    var data = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
     // .toDF(featuresArr: _*)
    return data
  }
}



