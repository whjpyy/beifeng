package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object HDFSFile {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("HDFSFile")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://spark1:9000/spark.txt")
    val count = lines.map(line => line.length)
      .reduce(_ + _)
    println("文本的字数：" + count)
  }
}
