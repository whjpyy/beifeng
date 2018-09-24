package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCountLocal {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("WordCountLocal")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:\\sparkTestFile\\spark.txt")
    lines.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .foreach(println)
  }
}
