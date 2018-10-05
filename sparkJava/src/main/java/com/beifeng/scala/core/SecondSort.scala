package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object SecondSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SecondSort")
      .setMaster("local")
    val sc = new SparkContext(conf)

    sc.textFile("D:\\sparkTestFile\\sort.txt")
      .map(line => {
        val arr = line.split(" ")
        (new SecondSortKey(arr(0).toInt, arr(1).toInt), line)
      })
      .sortByKey()
      .map(sorted => sorted._2)
      .foreach(println)
  }
}
