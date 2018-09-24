package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object SortWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SortWordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    sc.textFile("D:\\sparkTestFile\\spark.txt")
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map(t => (t._2, t._1))
      .sortByKey(false)
      .map(t => (t._2, t._1))
      .foreach(println)
  }

}
