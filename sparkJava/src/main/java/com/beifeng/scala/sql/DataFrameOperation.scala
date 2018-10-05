package com.beifeng.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameOperation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataFrameOperation")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("hdfs://spark1:9000/spark-sql/source/students.json")

    df.show()
    df.printSchema();
    df.select("name").show()
    df.select(df.col("name"), df.col("age").plus(1)).show()
    df.filter(df.col("age").gt(18)).show()
    df.groupBy(df.col("age")).count().show()

  }
}
