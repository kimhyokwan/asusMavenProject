package com.kopo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
object Model_Outlier {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("mavenProject").
      config("spark.master", "local").
      getOrCreate()

    val sampleData = List(10.2, 14.1,14.4,14.4,14.4,14.5,14.5,14.6,14.7,
      14.7, 14.7,14.9,15.1, 15.9,16.4)
    val rowRDD = spark.sparkContext.makeRDD(sampleData.map(x => Row(x)))

    val schema = StructType(Array(StructField("value",DoubleType)))
    val df = spark.createDataFrame(rowRDD,schema)

    val quantiles = df.stat.approxQuantile("value",
      Array(0.25,0.75),0.0)
    val Q1 = quantiles(0)
    val Q3 = quantiles(1)
    val IQR = Q3 - Q1

    val lowerRange = Q1 - 1.5*IQR
    val upperRange = Q3+ 1.5*IQR

    val outliers = df.filter(s"value < $lowerRange or value > $upperRange")
    outliers.show()
  }
}
