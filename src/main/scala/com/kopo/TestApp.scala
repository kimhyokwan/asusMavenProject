package com.kopo
import org.apache.spark.sql.SparkSession

object TestApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("mavenProject").
    config("spark.master", "local").
    getOrCreate()
    println("hello spark nice to meet you")
  }
}
