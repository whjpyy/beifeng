package com.beifeng.scala.sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object ParquetLoadData {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ParquetLoadData")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.load("hdfs://spark1:9000/spark-sql/source/users.parquet")

    df.registerTempTable("users")
    sqlContext.sql("select name from users").rdd
      .map(row => row.getString(0))
      .foreach(println)


  }
}
