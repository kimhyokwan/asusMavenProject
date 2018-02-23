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

// Functions for week Calculation
import java.util.Calendar
import java.text.SimpleDateFormat
import java.util.Date

object Model_DataCleansing {
  def main(args: Array[String]): Unit = {
    // User Defined Function
    // Purpose of Funtion : Calculate pre week
    def preWeek(inputYearWeek: String, gapWeek: Int): String = {
      val currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance();
      calendar.setMinimalDaysInFirstWeek(4);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);

      var dateFormat = new SimpleDateFormat("yyyyMMdd");

      calendar.setTime(dateFormat.parse(currYear + "1231"));
      //    calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (currWeek <= gapWeek) {
        var iterGap = gapWeek - currWeek
        var iterYear = currYear - 1

        calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"));
        var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        while (iterGap > 0) {
          if (iterWeek <= iterGap) {
            iterGap = iterGap - iterWeek
            iterYear = iterYear - 1
            calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))
            iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
          } else {
            iterWeek = iterWeek - iterGap
            iterGap = 0
          } // end of if
        } // end of while

        return iterYear.toString + "%02d".format(iterWeek)
      } else {
        var resultYear = currYear
        var resultWeek = currWeek - gapWeek

        return resultYear.toString + "%02d".format(resultWeek)
      } // end of if
    } // end of function

    // User Defined Function
    // Purpose of Funtion : Calculate post week
    def postWeek(inputYearWeek: String, gapWeek: Int): String = {
      val currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance();
      calendar.setMinimalDaysInFirstWeek(4);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);

      var dateFormat = new SimpleDateFormat("yyyyMMdd");

      calendar.setTime(dateFormat.parse(currYear + "1231"));

      var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (maxWeek < currWeek + gapWeek) {
        var iterGap = gapWeek + currWeek - maxWeek
        var iterYear = currYear + 1

        calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"));
        var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        while (iterGap > 0) {
          if (iterWeek < iterGap) {
            iterGap = iterGap - iterWeek
            iterYear = iterYear + 1
            calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))
            iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
          } else {
            iterWeek = iterGap
            iterGap = 0
          } // end of if
        } // end of while

        return iterYear.toString() + "%02d".format(iterWeek)
      } else {
        return currYear.toString() + "%02d".format((currWeek + gapWeek))
      } // end of if
    } // end of function
    def getquantileValue(inputData: Iterable[(Double)]): (Double, Double) = {

      val maxTarget_Array = inputData.toArray.sorted

      val quantileValue =
        if (maxTarget_Array.isEmpty) {
          0.0
        } else {
          val highValue = (maxTarget_Array.size - 1) * 0.75

          if (highValue >= maxTarget_Array.size - 1) {
            maxTarget_Array.last
          } else if (highValue <= 0) {
            maxTarget_Array(0)
          } else {
            val highValue_Int = Math.round(highValue).toInt

            if (highValue_Int == highValue) {
              maxTarget_Array(highValue_Int)
            } else {
              val highValue_Ceil_Int  = Math.ceil(highValue).toInt
              val highValue_Floor_Int = Math.floor(highValue).toInt

              (highValue_Ceil_Int - highValue) * maxTarget_Array(highValue_Floor_Int) + (highValue - highValue_Floor_Int) * maxTarget_Array(highValue_Ceil_Int)
            } // end of if
          } // end of if
        } // end of quantileValue
      val quantileValue2 =
        if (maxTarget_Array.isEmpty) {
          0.0
        } else {
          val lowValue = (maxTarget_Array.size - 1) * 0.25

          if (lowValue >= maxTarget_Array.size - 1) {
            maxTarget_Array.last
          } else if (lowValue <= 0) {
            maxTarget_Array(0)
          } else {
            val lowValue_Int = Math.round(lowValue).toInt

            if (lowValue_Int == lowValue) {
              maxTarget_Array(lowValue_Int)
            } else {
              val lowValue_Ceil_Int  = Math.ceil(lowValue).toInt
              val lowValue_Floor_Int = Math.floor(lowValue).toInt

              (lowValue_Ceil_Int - lowValue) * maxTarget_Array(lowValue_Floor_Int) + (lowValue - lowValue_Floor_Int) * maxTarget_Array(lowValue_Ceil_Int)
            } // end of if
          } // end of if
        } // end of quantileValue

      (quantileValue, quantileValue2)
    }
    val spark = SparkSession.builder().appName("mavenProject").
      config("spark.master", "local").
      getOrCreate()

    ///////////////////////////     Oracle 데이터 로딩 ////////////////////////////////////
    // 파일설정var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_result_data"

    // jdbc (java database connectivity) 연결
    val selloutDataFromOracle = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    selloutDataFromOracle.registerTempTable("selloutTable")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 2. data refining
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var selloutFilteredDf = spark.sql("select regionid, salesid, item, productgroup, product, yearweek, cast(qty as double), cast(target as double) from selloutTable where 1=1 and salesid = 'SALESID0101' and productgroup ='WM'")
    //var selloutFilteredDf = sqlContext.sql("select * from pro_actual_sales where productgroup = 'WM'")

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 3. data column indexing & make rdd
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var selloutColumnIndex = selloutFilteredDf.columns.map(x=>{x.toLowerCase()})

    var ap2idIndex = selloutColumnIndex.indexOf("regionid")
    var salesidIndex = selloutColumnIndex.indexOf("salesid")
    var itemIndex = selloutColumnIndex.indexOf("item")
    var productgroupIndex = selloutColumnIndex.indexOf("productgroup")
    var productIndex = selloutColumnIndex.indexOf("product")
    var yearweekIndex = selloutColumnIndex.indexOf("yearweek")
    var qtyIndex = selloutColumnIndex.indexOf("qty")

    var selloutRdd = selloutFilteredDf.rdd
    var currWeek = selloutRdd.map(x=>{ x.getString(yearweekIndex)}).max //postWeek(inputs(1).first.getString(0),1) //selloutRdd.map(x=>{ x.getString(yearweekIndex)}).max
    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 4. data Cleansing
    //////////////////////////////////////////////////////////////////////////////////////////////////

    var cleansingDataRddFirst = selloutRdd.
      groupBy(x=>{
        (x.getString(salesidIndex), x.getString(itemIndex)) }).
      filter(x=>{

        var checkValid = true
        var key = x._1
        var data = x._2

        var dataSize = data.size
        if(dataSize < 26) checkValid = false
        checkValid})

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 6. data transforming - flatMap
    //////////////////////////////////////////////////////////////////////////////////////////////////

    var cleansingDataRddSecond = cleansingDataRddFirst.flatMap(x=>{
      var key = x._1
      var data = x._2

      var ap2id = data.map(x=>{x.getString(ap2idIndex)}).head
      var productgroup = data.map(x=>{x.getString(productgroupIndex)}).head
      var product = data.map(x=>{x.getString(productIndex)}).head

      var sortedData = data.toSeq.sortBy(x=>{x.getString(yearweekIndex).toInt})
      var averageValue = sortedData.takeRight(4).map(x=>{x.getDouble(qtyIndex)}).sum/4

      //////////////////////////////////////////////////////////////////////////////////////////////////
      //////////////////////////////// Fill the logic //////////////////////////////////////////////////
      //////////////////////////////////////////////////////////////////////////////////////////////////
      var qtyList = data.map(x=>{ x.getDouble(qtyIndex)})
      var q1 = getquantileValue(qtyList)._2
      var q3 = getquantileValue(qtyList)._1
      var iqr = q3-q1
      var lowerRange = q1-1.5*iqr
      var upperRange = q3+1.5*iqr
      var outlierData = data.
        map(x=>{
          var qty = x.getDouble(qtyIndex)
          if(qty > upperRange){qty = upperRange}
          //////////////////////////////////////////////////////////////////////////////////////////////////
          //////////////////////////////// Fill the logic //////////////////////////////////////////////////
          //////////////////////////////////////////////////////////////////////////////////////////////////

          Row(x.getString(salesidIndex), x.getString(itemIndex), x.getString(yearweekIndex), x.getDouble(qtyIndex), qty, averageValue, ap2id, productgroup, product, currWeek) })

      outlierData
    })
    // count 116546

    //////////////////////////////////////////////////////////////////////////////////////////////////
    // 6. data transforming - flatMap & output
    //////////////////////////////////////////////////////////////////////////////////////////////////
    var flatMapOutputDataFrame = spark.createDataFrame(cleansingDataRddSecond,
      StructType(Seq(
        StructField("SALESID", StringType),
        StructField("ITEM", StringType),
        StructField("YEARWEEK", StringType),
        StructField("QTY_ORG", DoubleType),
        StructField("QTY_NEW", DoubleType),
        StructField("AVERAGE4WEEK",DoubleType),
        StructField("REGIONID", StringType),
        StructField("PRODUCTGROUP", StringType),
        StructField("PRODUCT", StringType),
        StructField("PLANWEEK", StringType)
      )))
    var cleansingResult = flatMapOutputDataFrame.selectExpr("SALESID", "ITEM", "YEARWEEK", "AVERAGE4WEEK", "REGIONID", "PRODUCTGROUP", "PRODUCT", "PLANWEEK", "stack(2, 'QTY_ORG', QTY_ORG, 'QTY_NEW', QTY_NEW) as (MEASURE, QTY)")
   // var cleansingResult = flatMapOutputDataFrame.select($"SALESID", $"ITEM", $"YEARWEEK", $"AVERAGE4WEEK", $"REGIONID", $"PRODUCTGROUP", $"PRODUCT", $"PLANWEEK", expr("stack(2, 'QTY_ORG',QTY_ORG,'QTY_NEW',QTY_NEW) as (MEASURE, QTY)"))
    var prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    var table = "kopo_channel_cleaning_hk"
    //append
    cleansingResult.write.mode("overwrite").jdbc(staticUrl, table, prop)
  }
}
