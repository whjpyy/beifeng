package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object LineCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("LineCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    sc.textFile("D:\\sparkTestFile\\hello.txt")
      .map(line => (line, 1))
      .reduceByKey(_ + _)
      .foreach(println)
  }
}
