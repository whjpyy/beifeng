package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorVariable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("AccumulatorVariable")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val sum = sc.accumulator(0)
    sc.parallelize(Range(1, 6))
        .foreach(sum.add(_))

    println(sum.value)
  }

}
