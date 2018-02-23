package com.kopo

import org.apache.spark.sql.SparkSession

object Model_DataUnloading {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("mavenProject").
      config("spark.master", "local").
      getOrCreate()

    ///////////////////////////     데이터 파일 로딩 ////////////////////////////////////
    // 파일설정
    var dataPath = "./data/"
    var selloutFile = "kopo_channel_seasonality.csv"

    // 상대경로 입력
    var selloutData1 = spark.read.format("csv").option("header", "true").load("c://spark-2.0.2/bin/data/" + selloutFile)

    // 절대경로 입력
    var selloutData2 = spark.read.format("csv").option("header", "true").load("c://spark-2.0.2/bin/data/" + selloutFile)

    // 메모리 테이블 생성
    selloutData1.registerTempTable("selloutTable")
    println(selloutData1.show)

    selloutData1.
      coalesce(1). // 파일개수
      write.format("csv"). // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("selloutFileResult.csv") // 저장파일명

    // 데이터베이스 주소 및 접속정보 설정
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    var staticUser = "kopo"
    var staticPw = "kopo"

    // 데이터 저장
    // oracle
    var prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    var table = "kopo_channel_seasonality_hk"
    //append
    selloutData1.write.mode("overwrite").jdbc(staticUrl, table, prop)

    // 데이터 저장
    // postgresql
    var postgresUrl = "jdbc:postgresql://127.0.0.1:5432/postgres"
    var postgresUser = "postgres"
    var postgrePw = "brightics"
    prop = new java.util.Properties
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", postgresUser)
    prop.setProperty("password", postgrePw)
    table = "kopo_channel_seasonality_hk"
    //append
    selloutData1.write.mode("overwrite").jdbc(postgresUrl, table, prop)

    // mysql
    var mysqlUrl = "jdbc:mysql://127.0.0.1:3306/kopo"
    var mysqlUser = "kopo"
    var mysqlPw = "kopo"
    prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", mysqlUser)
    prop.setProperty("password", mysqlPw)
    table = "kopo_channel_seasonality_hk"
    //append
    selloutData1.write.mode("overwrite").jdbc(mysqlUrl, table, prop)
  }
}
