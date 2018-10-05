package com.beifeng.scala.sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object ParquetPartitionDiscovery {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ParquetPartitionDiscovery")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.load("hdfs://spark1:9000/spark-sql/source/users/gender=male/country=US/users.parquet")

    df.show()
    df.printSchema()

  }
}
