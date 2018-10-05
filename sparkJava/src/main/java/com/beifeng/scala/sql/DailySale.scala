package com.beifeng.scala.sql

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._

object DailySale {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DailySale")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 模拟数据
    val userSaleLog = Array(
      "2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123"
    )
    val rddUserLogRows = sc.parallelize(userSaleLog, 5)
      .filter(log => log.split(",").length == 3)
      .map(log => {
        val arr = log.split(",")
        Row(arr(0), arr(1).toDouble)
      })

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("sale_amount", DoubleType, true)
    ))
    val dfUserSaleLog = sqlContext.createDataFrame(rddUserLogRows, structType)

    dfUserSaleLog.groupBy("date")
      .agg('date, sum('sale_amount))
      .map { row => Row(row(1), row(2)) }
      .collect()
      .foreach(println)

  }
}
