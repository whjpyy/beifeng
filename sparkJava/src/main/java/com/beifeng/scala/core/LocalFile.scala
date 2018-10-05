package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object LocalFile {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("LocalFile")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("D:\\sparkTestFile\\spark.txt")
    val count = lines.map(line => line.length)
      .reduce(_ + _)
    println("文本的字数：" + count)
  }
}
