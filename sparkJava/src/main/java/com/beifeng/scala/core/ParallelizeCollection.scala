package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object ParallelizeCollection {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("ParallelizeCollection")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Range(1, 11)
    val sum = sc.parallelize(numbers).reduce(_ + _)
    println(sum)
  }
}
