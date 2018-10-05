package com.beifeng.scala.sql

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RowNumberWindowFunction {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RowNumberWindowFunction")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val rddSaleRows = sc.textFile("D:\\SparkTestFile\\sales.txt")
      .map(line => {
        val arr = line.split("\u0001")
        Row(arr(0), arr(1), arr(2).toInt)
      })

    val structType = StructType(Array(
      StructField("product", StringType, true),
      StructField("category", StringType, true),
      StructField("revenue", IntegerType, true)
    ))
    val dfSales = hiveContext.createDataFrame(rddSaleRows, structType)
    dfSales.registerTempTable("sales")
    dfSales.show()

    val dfTop3Sales = hiveContext.sql(
      "select product,category,revenue" +
        " from (" +
        "   select product, category, revenue, " +
        "   row_number() over (partition by category order by revenue desc) rank " +
        "   from sales" +
        ") tmp_sales " +
        " where rank <= 3")
    dfTop3Sales.show()

  }
}
