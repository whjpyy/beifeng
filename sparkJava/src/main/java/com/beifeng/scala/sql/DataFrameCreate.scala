package com.beifeng.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameCreate {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataFrameCreate")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("hdfs://spark1:9000/spark-sql/source/students.json")
    df.show()


  }
}
