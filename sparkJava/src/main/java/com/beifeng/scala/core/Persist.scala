package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object Persist {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Persist")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:\\sparkTestFile\\spark.txt").cache()
    var beginTime = System.currentTimeMillis()
    var count = lines.count()
    println(count)
    var endTime = System.currentTimeMillis()
    println("cost " + (endTime - beginTime) + " milliseconds")


    beginTime = System.currentTimeMillis()
    count = lines.count()
    println(count)
    endTime = System.currentTimeMillis()
    println("cost " + (endTime - beginTime) + " milliseconds")
  }
}
