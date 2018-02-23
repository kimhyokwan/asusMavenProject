package com.kopo

import org.apache.spark.sql.SparkSession

object Model_DataLoading {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("mavenProject").
      config("spark.master", "local").
      getOrCreate()

    ///////////////////////////     데이터 파일 로딩 ////////////////////////////////////
    // 파일설정
    var dataPath = "./data/"
    var selloutFile = "kopo_channel_seasonality.csv"

    // 상대경로 입력
    var selloutData1 = spark.read.format("csv").option("header", "true").load(dataPath + selloutFile)

    // 절대경로 입력
    var selloutData2 = spark.read.format("csv").option("header", "true").load("c:/spark-2.0.2/bin/data/" + selloutFile)

    // 메모리 테이블 생성
    selloutData1.registerTempTable("selloutTable")
    println(selloutData1.show)

    ///////////////////////////     데이터 파일 로딩 ////////////////////////////////////
    // 파일설정
    dataPath = "./data/"
    selloutFile = "kopo_batch_season_mpara.csv"

    // 상대경로 입력
    selloutData1 = spark.read.format("csv").option("delimiter", ";").option("header", "true").load("c:/spark-2.0.2/bin/data/" + selloutFile)
      //.option("header", "true").load(dataPath + selloutFile)

    // 절대경로 입력
    selloutData2 = spark.read.format("csv").option("delimiter", ";").option("header", "true").load("c:/spark-2.0.2/bin/data/" + selloutFile)

    // 메모리 테이블 생성
    selloutData1.registerTempTable("selloutTable")
    println(selloutData1.show)

    ///////////////////////////     Oracle 데이터 로딩 ////////////////////////////////////
    // 파일설정var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality"

    // jdbc (java database connectivity) 연결
    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutDataFromOracle.registerTempTable("selloutTable")

  }
}
