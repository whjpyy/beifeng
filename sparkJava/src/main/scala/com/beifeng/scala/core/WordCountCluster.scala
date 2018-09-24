package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCountCluster {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCountCluster")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://spark1:9000/spark.txt")

    lines.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .foreach(println)

  }

}
