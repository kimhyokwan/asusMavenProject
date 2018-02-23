package com.kopo

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.apache.commons.math3.stat.descriptive.SummaryStatistics

import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

object Model_Seasonality {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession
    import scala.collection.mutable.ArrayBuffer

    import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
    import org.apache.spark.sql.types._

    import org.apache.spark.rdd.RDD
    import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
    import org.apache.commons.math3.stat.descriptive.SummaryStatistics

    import scala.collection.mutable.ArrayBuffer
    import org.apache.spark.rdd.RDD
    import org.apache.spark.rdd.RDD.rddToOrderedRDDFunctions
    import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
    import org.apache.spark.sql.DataFrame
    import org.apache.spark.sql.Row

    // Define library Related to RDD Registry
    import org.apache.spark.sql.types.StructType
    import org.apache.spark.sql.types.StringType
    import org.apache.spark.sql.types.StructField
    import org.apache.spark.sql.types.DoubleType
    import org.apache.spark.sql.types.TimestampType
    import org.apache.spark.sql.types.DateType

    // Functions for week Calculation
    import java.util.Calendar
    import java.text.SimpleDateFormat
    import java.util.Date

    def movingAverage(targetData: Iterable[Double], myorder: Int): List[Double] = {
      val length = targetData.size
      if (myorder > length || myorder <= 2) {
        throw new IllegalArgumentException
      } else {
        var maResult = targetData.sliding(myorder).map(_.sum).map(_ / myorder)

        if (myorder % 2 == 0) {
          maResult = maResult.sliding(2).map(_.sum).map(_ / 2)
        }

        maResult.toList
      }
    }

    //myResult = myResult ++ getSeasonality(splitData, myorder, myIter, qtyColumnNum, yearweekColumnNum)
    def getSeasonality(myproduct: Iterable[(Row, String)], myorder: Int, myIter: Int, useQtyColumnNum: Int, yearweekColumnNum: Int): Iterable[((Row, String), Double, Double, Double, Double, Double)] = {
      val subOrder = (myorder.toDouble / 2.0).floor.toInt
      var rowQtySeqData = myproduct.toSeq.sortBy(x => {x._1.getString(yearweekColumnNum).toInt}).map(x => { x._1.getDouble(useQtyColumnNum) }).zipWithIndex
      //var rowQtySeqData = myproduct.map(x => { x._1.getDouble(useQtyColumnNum) }).zipWithIndex
      val countData = myproduct.size

      var finalOTMA = new ArrayBuffer[Double]
      var finalSD = new ArrayBuffer[Double]

      for (iter <- 0 until myIter) {

        //*******************************************************
        //*******************************************************
        var OTMA = movingAverage(rowQtySeqData.map(x => x._1), myorder)

        val preMAArray = new ArrayBuffer[Double]
        var postMAArray = new ArrayBuffer[Double]

        for (j <- 0 until subOrder) {
          //(0~j+suborder).mean

          preMAArray.append(rowQtySeqData.filter(x => x._2 >= 0 && x._2 <= (j + subOrder)).map(x => x._1).sum / (j + subOrder + 1).toDouble)
          postMAArray.append(rowQtySeqData.filter(x => x._2 >= (countData.toInt - j - (subOrder) - 1) && x._2 < (countData)).map(x => x._1).sum / (countData.toInt - (countData.toInt - j - (subOrder) - 1)).toDouble)

        }

        postMAArray = postMAArray.reverse

        finalOTMA = preMAArray.union(OTMA)
        finalOTMA = finalOTMA.union(postMAArray)

        //*******************************************************
        //*******************************************************
        val sdArray = new ArrayBuffer[Double]
        for (j <- 4 until (countData.toInt - 4)) {
          var filteredData = rowQtySeqData.filter(x => x._2 >= j - 4 && x._2 <= j + 4).map(x => x._1)
          var mean = filteredData.sum / filteredData.size
          var stddev = Math.sqrt(filteredData.map(x => { Math.pow(x - mean, 2.0) }).sum / (filteredData.size - 1.0))
          sdArray.append(stddev)
        }

        val sdPreArray = new ArrayBuffer[Double]
        var sdPostArray = new ArrayBuffer[Double]
        for (j <- 0 until 4) {

          var filteredData = rowQtySeqData.filter(x => x._2 >= 0 && x._2 <= j + 4).map(x => x._1)
          var mean = filteredData.sum / filteredData.size
          var stddev = Math.sqrt(filteredData.map(x => { Math.pow(x - mean, 2.0) }).sum / (filteredData.size - 1.0))

          sdPreArray.append(stddev)

          var filteredData2 = rowQtySeqData.filter(x => x._2 >= (countData - j - 4 - 1) && x._2 < countData).map(x => x._1)

          var mean2 = filteredData2.sum / filteredData2.size
          var stddev2 = Math.sqrt(filteredData2.map(x => { Math.pow(x - mean2, 2.0) }).sum / (filteredData.size - 1.0))
          sdPostArray.append(stddev2)
        }
        sdPostArray = sdPostArray.reverse

        finalSD = sdPreArray.union(sdArray)
        finalSD = finalSD.union(sdPostArray)

        rowQtySeqData = rowQtySeqData.map(x => {
          if (x._1 > finalOTMA(x._2.toInt) + finalSD(x._2.toInt) || x._1 < finalOTMA(x._2.toInt) - (2.0 * finalSD(x._2.toInt))) {
            (finalOTMA(x._2.toInt), x._2)
          } else {
            (x._1, x._2)
          }
        })
      }

      //**************************************************************************************
      //**************************************************************************************
      val MA = movingAverage(rowQtySeqData.map(x => x._1), 9)
      val preMAArray = new ArrayBuffer[Double]
      var postMAArray = new ArrayBuffer[Double]
      for (j <- 0 until 4) {
        preMAArray.append(rowQtySeqData.filter(x => x._2 >= 0 && x._2 <= (j + 4)).map(x => x._1).sum / (j + 5).toDouble)

        postMAArray.append(rowQtySeqData.filter(x => x._2 >= (countData - j - 4 - 1) && x._2 < (countData)).map(x => x._1).sum / (countData - (countData - j - 4 - 1)).toDouble)

      }
      postMAArray = postMAArray.reverse

      var finalMA = preMAArray.union(MA)
      finalMA = finalMA.union(postMAArray)

      val result = myproduct.zip(rowQtySeqData).map(data => {

        val rawSeasonality = data._1._1.getDouble(useQtyColumnNum) / finalMA(data._2._2.toInt) - 1.0

        (data._1, data._2._1, finalOTMA(data._2._2.toInt), finalSD(data._2._2.toInt), finalMA(data._2._2.toInt), rawSeasonality)
      })

      result

    }
    val spark = SparkSession.builder().appName("mavenProject").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    var staticUser = "kopo"
    var staticPw = "kopo"
    val paramIn= spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> "kopo_batch_season_mpara","user" -> staticUser, "password" -> staticPw)).load
    paramIn.registerTempTable("paramTable")
    val season_parameters = spark.sql("select * from paramTable")

    val myorder =  season_parameters.rdd.filter(row => row.getString(0).equalsIgnoreCase("myorder")).map(x => x.getString(1)).first().toInt
    val myIter =  season_parameters.rdd.filter(row => row.getString(0).equalsIgnoreCase("myIter")).map(x => x.getString(1)).first().toInt

    val jobid = season_parameters.rdd.filter(row => row.getString(0).equalsIgnoreCase("jobid")).map(x => x.getString(1)).first().toString()
    val prefix =  season_parameters.rdd.filter(row => row.getString(0).equalsIgnoreCase("prefix")).map(x => x.getString(1)).first().toString()
    val resultTableName =  prefix + "_" + jobid + "_season_result"

    //STEP2: Get past sales data(3year) then set column's index information
    //val pastSalesDataDF = inputs(0)
    val salesIin= spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> "kopo_channel_seasonality","user" -> staticUser, "password" -> staticPw)).load
    salesIin.registerTempTable("salesTable")
    val pastSalesDataDF = spark.sql("select regionid, productgroup, product, yearweek, year, week, cast(qty as double) as qty from salesTable")

    val season_index = pastSalesDataDF.columns.map(x=>{x.toLowerCase})

    val productColumnNum = season_index.indexOf("product")
    val qtyColumnNum = season_index.indexOf("qty")
    val regionidColumnNum = season_index.indexOf("regionid")
    val productgColumnNum = season_index.indexOf("productgroup")
    val weekColumnNum = season_index.indexOf("week")
    val yearweekColumnNum = season_index.indexOf("yearweek")
    val yearColumnNum = season_index.indexOf("year")

    val pastSalesDataRDD = pastSalesDataDF.rdd
    val curryear =  pastSalesDataRDD.map(x=>{x.getString(yearColumnNum)}).max.toLong

    val ap2idList = pastSalesDataRDD.map(x => { x.getString(regionidColumnNum) }).distinct().collect.mkString(",")
    val productgroupList = pastSalesDataRDD.map(x => { x.getString(productgColumnNum) }).distinct().collect.mkString(",")
    val productGroupListSplit = productgroupList.split(",")
    val ap2idListSplit = ap2idList.split(",")

    var ap2idListSet = ap2idListSplit.toSet
    var productGroupListSplitSet = productGroupListSplit.toSet

    //STEP3: Extract seasonality distributed processing
    var resultUnion = pastSalesDataRDD.filter(row => {
      var checkValid = false

      // Filtering ap2id&productgroup
      if(ap2idListSet.contains(row.getString(regionidColumnNum))){checkValid = true}
      if(productGroupListSplitSet.contains(row.getString(productgColumnNum))){checkValid = true}

      checkValid && !row.getString(weekColumnNum).equalsIgnoreCase("53")
    }).
      groupBy { x => (x.getString(regionidColumnNum), x.getString(productgColumnNum), x.getString(yearColumnNum)  ) }.
      filter(splitRow=>{

        //Debugging point#1
        /*
        var splitRow = resultUnion.filter(x=>{
          (x._1._1 == "300117") &&
          (x._1._2 == "REF") &&
          (x._1._3 == "2015") }).first
          */
        var checkValid = true

        var inputdata = splitRow._2

        var distinctYearweek = inputdata.map { x => x.getString(yearweekColumnNum) }.toSeq.distinct

        var yearweekSize = distinctYearweek.size

        for (i <- 0 until yearweekSize){
          var currYearweek = distinctYearweek(i)
          var pgQtySum = inputdata.
            //filter { x => x.getString(yearweekColumnNum).equalsIgnoreCase(currYearweek) && x.getDouble(qtyColumnNum) < 1 }
            filter(x=>{ x.getString(yearweekColumnNum).equalsIgnoreCase(currYearweek)}).map(x=>{x.getDouble(qtyColumnNum)}).sum
          if (pgQtySum == 0) {
            checkValid = false
          }
        }
        checkValid

      }).flatMap(x=>{
      x._2
    }).
      // Grouping (ap2id, produtgroup)
      groupBy { x => (x.getString(regionidColumnNum), x.getString(productgColumnNum)) }.
      // Distributed processing
      flatMap(splitRow => {
      //Debugging point#1
      /*
      var splitRow = resultUnion.filter(x=>{
        (x._1._1 == "A01") &&
        (x._1._2 == "REF") }).first
        */
      val inputMyData = splitRow._2

      // 1. Get all product information
      val distinctProduct = inputMyData.map { x => x.getString(productColumnNum) }.toSeq.distinct

      val productInfo = new ArrayBuffer[(String, String)]

      // 2. Check for normal-data
      for (i <- 0 until distinctProduct.length) {
        val currProductName = distinctProduct(i)
        val flagCheck = inputMyData.filter { x => x.getString(productColumnNum).equalsIgnoreCase(currProductName) &&
          x.getDouble(qtyColumnNum) < 1 }

        if (flagCheck.size > 0) {
          productInfo.append((distinctProduct(i), "G"))
        } else {
          productInfo.append((distinctProduct(i), "P"))
        }
      }
      var pp = 0
      var gg = 0

      var myResult: Iterable[((Row, String), Double, Double, Double, Double, Double)] = Iterable.empty
      var mysave: Iterable[(Row, String)] = Iterable.empty
      var mysave2: Iterable[(Row, String)] = Iterable.empty

      for (i <- 0 until distinctProduct.length) {
        val currProductName = distinctProduct(i)
        val currProductInfo = productInfo(i)
        val splitData = inputMyData.filter { x => x.getString(productColumnNum).equalsIgnoreCase(currProductName) }.
          map { x => (x, currProductInfo._2) }.
          toSeq.sortBy(x=>{x._1.getString(yearweekColumnNum)})

        if (currProductInfo._2.equalsIgnoreCase("P")) {
          pp = 1
          mysave = mysave ++ splitData
          //20160622 revision for sort
          myResult = myResult ++ getSeasonality(splitData, myorder, myIter, qtyColumnNum, yearweekColumnNum)

        }
        else {
          gg = 1
        }
      }
      var seasonalityP: Iterable[(String, String, Double, String)] = Iterable.empty
      var seasonalityG: Iterable[(String, String, Double, String)] = Iterable.empty
      var seasonalityGP: Iterable[(String, String, Double, String)] = Iterable.empty

      if (pp == 1) {

        seasonalityP = myResult.map(data => {
          ((data._1._1.getString(weekColumnNum), data._1._1.getString(productColumnNum)), (1l, data._6))
        }).groupBy(x => x._1).map(data => {
          (data._1._2, data._1._1, data._2.map(x => x._2._2).sum.toDouble / data._2.size.toDouble, "P")
        })
      }

      if (gg == 1) {
        if (pp == 1) {

          mysave = mysave.map(data => {
            ((data._1.getString(productgColumnNum), data._1.getString(yearweekColumnNum), data._1.getString(yearColumnNum), data._1.getString(weekColumnNum), data._2), (data._1.getDouble(qtyColumnNum), data._1.getString(regionidColumnNum), data._1.getString(productColumnNum)))
          }).groupBy(x => x._1).map(data => {
            // ((0, 0, 0, 0, 0), (0, 0, 0))
            val currValue = data._2.reduce((v1, v2) => { (v1._1, (v1._2._1 + v2._2._1, v1._2._2, v1._2._3)) })
            (Row.fromSeq(Array(currValue._2._2, data._1._1, currValue._2._3, data._1._2, data._1._3, data._1._4, currValue._2._1.toDouble)), data._1._5)
            // (ap2id,productgroup,product,yearweek,year,week,qty), flag
          })

          mysave2 = mysave.toSeq.sortBy(x=>{(x._1.getString(1),x._1.getString(3))})
          // 20160622 revision for sort
          myResult = getSeasonality(mysave2, myorder, myIter, qtyColumnNum, yearweekColumnNum)

          seasonalityG = myResult.map(data => {
            ((data._1._1.getString(weekColumnNum), data._1._1.getString(productgColumnNum)), (1l, data._6))
          }).groupBy(x => x._1).map(data => {
            (data._1._2, data._1._1, data._2.map(x => x._2._2).sum.toDouble / data._2.size.toDouble, "G")
          })
        }
        else {

          var acc = 1
          val initBuffer = new ArrayBuffer[(String, String, Double, String)]

          for (j <- 0 until 52) {
            initBuffer.append((distinctProduct(0), acc.toString(), 0.0, "G"))
            acc = acc + 1
          }

          seasonalityG = initBuffer.toIterable
        }

        for (i <- 0 until distinctProduct.length) {
          if (productInfo(i)._2.equalsIgnoreCase("G")) {
            val tempG = seasonalityG.map(x => {
              (productInfo(i)._1, x._2, x._3, x._4)
            })
            seasonalityGP = tempG ++ seasonalityGP
          }
        }
      }

      var finalResult: Iterable[(String, String, Double, String)] = Iterable.empty

      if (gg == 1 && pp == 1) {
        finalResult = seasonalityP ++ seasonalityGP
      } else if (pp == 1 && gg == 0) {
        finalResult = seasonalityP
      } else if (pp == 0 && gg == 1) {
        finalResult = seasonalityGP
      }

      var addWeek: Iterable[(String, String, Double, String)] = Iterable.empty
      for (i <- 0 until distinctProduct.length) {
        val currProductName = distinctProduct(i)
        val currWeek1 = finalResult.filter(data => { data._1.equalsIgnoreCase(currProductName) && (data._2.equalsIgnoreCase("1") || data._2.equalsIgnoreCase("01")) })
        val currWeek52 = finalResult.filter(data => { data._1.equalsIgnoreCase(currProductName) && data._2.equalsIgnoreCase("52") })

        val week53Seasonality = (currWeek1.head._3 + currWeek52.head._3) / 2.0
        val week53Data = currWeek1.take(1).map(x=>{ (x._1, "53", week53Seasonality, x._4)})
        addWeek = week53Data ++ addWeek
      }

      finalResult = finalResult.filter(data => {
        data._2.toInt < 53
      }) ++ addWeek

      finalResult.map(data => {
        Row.fromSeq(Seq(data._1, String.valueOf(data._2.toInt), data._3, data._4, splitRow._1._1, splitRow._1._2,""))
      })
      // (PRODUCT, WEEK, seasonality, flag, ap2id, productgroup)

      //////////////////////
    })//.sortBy(x => (x.getString(0),x.getString(1).toInt), true, 1)

    val finalResultDF = spark.createDataFrame(resultUnion,
      StructType(Seq(StructField("PRODUCT", StringType),
        StructField("WEEK", StringType),
        StructField("SEASONALITY", DoubleType),
        StructField("FLAG", StringType),
        StructField("REGIONID", StringType),
        StructField("PRODUCTGROUP", StringType),
        StructField("REGIONNAME", StringType))))
    //    // BaaS convert
    //    finalResultDF
    //    finalResultDF.coalesce(1).write.format("csv").option("header", "true").save("result3.csv")
    //    println("seasonality csv import completed")

    //var resultTableName = "kopo_batch_season_result"

    // Saving data to a JDBC source
//    finalResultDF.write.
//      mode(SaveMode.Overwrite).
//      format("jdbc").
//      option("url", staticUrl).
//      option("dbtable", "kopo_batch_season_result").
//      option("user", staticUser).
//      option("password", staticPw).
//      save()


    //val prop = new java.util.Properties
    //prop.setProperty("driver", "org.postgresql.Driver")
    //prop.setProperty("user", "postgres")
    //prop.setProperty("password", "kopo")
    //jdbc mysql url - destination database is named "data"
    //val url = "jdbc:postgresql://127.0.0.1:5432/postgres"
    //destination database table
    //val table = "outresult"
    //finalResultDF.write.mode(SaveMode.Overwrite).jdbc(url, table, prop)

//    val prop = new java.util.Properties
//    prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver")
//    prop.setProperty("user", "kopo")
//    prop.setProperty("password", "kopo")
//    //jdbc mysql url - destination database is named "data"
//    //destination database table
//    val table = "outresult2"
//    finalResultDF.write.mode(SaveMode.Overwrite).jdbc(staticUrl, table, prop)

        val prop = new java.util.Properties
        prop.setProperty("driver", "oracle.jdbc.OracleDriver")
        prop.setProperty("user", staticUser)
        prop.setProperty("password", staticPw)
        val table = "kopo_season_result"
    //append
        finalResultDF.write.mode("overwrite").jdbc(staticUrl, table, prop)
        println("seasonality oracle import completed")
  }
}
