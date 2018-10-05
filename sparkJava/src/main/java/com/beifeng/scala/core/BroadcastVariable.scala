package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariable {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BroadcastVariable")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val factor = 3;
    val bcFactor = sc.broadcast(factor)
    sc.parallelize(Range(1, 6))
      .map(_ * bcFactor.value)
      .foreach(println)
  }

}
