package com.beifeng.scala.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object DailyUv {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DailyUv")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    // 模拟数据
    val userSaleLog = Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1121",
      "2015-10-02,1123",
      "2015-10-02,1123"
    )
    val rddUserLogRows = sc.parallelize(userSaleLog, 5)
      .map(log => {
        val arr = log.split(",")
        Row(arr(0), arr(1).toInt)
      })

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("userId", IntegerType, true)
    ))
    val dfUserSaleLog = sqlContext.createDataFrame(rddUserLogRows, structType)

    dfUserSaleLog.groupBy("date")
      .agg(count('userId), countDistinct('userId))
      .rdd.foreach(println)

  }
}
