package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object Top3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Top3")
      .setMaster("local")
    val sc = new SparkContext(conf)

    sc.textFile("D:\\sparkTestFile\\top.txt")
      .map(s => (s.toInt, s))
      .sortByKey(false)
      .map(_._2)
      .take(3)
      .foreach(println)
  }
}
