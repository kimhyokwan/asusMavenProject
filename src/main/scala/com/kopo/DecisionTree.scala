package com.kopo

object DecisionTree {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
    import org.apache.spark.ml.regression.{DecisionTreeRegressionModel, DecisionTreeRegressor}
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
    import org.apache.commons.math3.stat.descriptive.SummaryStatistics
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder().appName("mavenProject").
      config("spark.master", "local").
      getOrCreate()

    var staticUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"
    var staticUser = "kopo"
    var staticPw = "kopo"
    val decisionIn = spark.read.format("jdbc").options(Map("url" -> staticUrl, "dbtable" -> "KOPO_DECISION_TREE_ALL", "user" -> staticUser, "password" -> staticPw)).load
    decisionIn.registerTempTable("decisionInputTable")

    var targetItem = "WA45H7000AW/A2"
    var targetYear = "201630"

    val decisionTreeInputDF = spark.sql("select regionid, productgroup, product, item, yearweek, year, cast(week as double) as week , " +
      "cast(qty as double), holiday, hclus, " +
      "promotion" +
      ", cast(pro_percent as double)," +
      "case when promotion = 'Y' then 1 else 0 end as promotioncode," +
      "case when holiday = 'Y' then 1 else 0 end as holidaycode" +
      " from decisionInputTable" +
      " where yearweek between '201501' and '201652'" +
      "and item = '" + targetItem + "'")

    //var assembler = new VectorAssembler().setInputCols(Array("holidaycode",  "promotioncode", "pro_percent","hcode")).setOutputCol("features")
    var assembler = new VectorAssembler().setInputCols(Array("week", "pro_percent", "holidaycode")).setOutputCol("features")

    var dt = new DecisionTreeRegressor().setLabelCol("qty").setFeaturesCol("features")

    // val pipeline = new Pipeline().setStages(Array(salesidIndexer, pgIndexer, holidayIndexer, promotionIndexer, assembler, dt ))
    val pipeline = new Pipeline().setStages(Array(assembler, dt))

    val train = decisionTreeInputDF.filter(decisionTreeInputDF("item") === targetItem && decisionTreeInputDF("yearweek") <= targetYear)
    val test = decisionTreeInputDF.filter(decisionTreeInputDF("item") === targetItem && decisionTreeInputDF("yearweek") > targetYear)

    val model = pipeline.fit(train)

    val predictions = model.transform(test)

    predictions.orderBy("yearweek").show

    val evaluatorRmse = new RegressionEvaluator().setLabelCol("qty").setPredictionCol("prediction").setMetricName("rmse")
    val evaluatorMae = new RegressionEvaluator().setLabelCol("qty").setPredictionCol("prediction").setMetricName("mae")
    val rmse = evaluatorRmse.evaluate(predictions)
    val mae = evaluatorMae.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)
    println("Mean Average Error (MAE) on test data = " + mae)

    val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]
    println("Learned regression tree model:\n" + treeModel.toDebugString)

    val df1 = predictions.withColumn("measure",lit("real-qty")).registerTempTable("df1")
    val df2 = predictions.withColumn("measure",lit("prediction-qty")).registerTempTable("df2")

    var finalResult = spark.sql("select measure as MEASURE, yearweek as YEARWEEK, qty as SALES, item as ITEM, productgroup as PRODUCTGROUP from df1 union " +
      "select measure, yearweek, prediction as sales, item, productgroup from df2")

    val prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", staticUser)
    prop.setProperty("password", staticPw)
    val table = "dt_result_final"
    finalResult.write.mode("overwrite").jdbc(staticUrl, table, prop)
    println("Decisiontree oracle import completed")
  }
}






