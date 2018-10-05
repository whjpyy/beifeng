package com.beifeng.scala.core

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object ActionOperation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ActionOperation")
      .setMaster("local")

    val sc = new SparkContext(conf)

    reduce(sc)
    collect(sc)
    count(sc)
    take(sc)
    saveAsTextFile(sc)
    countByKey(sc)
  }

  def reduce(sc: SparkContext) = {
    val sum = sc.parallelize(Range(1, 11))
      .reduce(_ + _)
    println(sum)

  }

  def collect(sc: SparkContext) = {
    val listNumbers = sc.parallelize(Range(1, 11))
      .map(_ * 2)
      .collect()
    for (i <- listNumbers) {
      println(i)
    }
  }

  def count(sc: SparkContext) = {
    val count = sc.parallelize(Range(1, 11))
      .count()
    println(count)
  }

  def take(sc: SparkContext) = {
    val top3Number = sc.parallelize(Range(1, 11))
      .take(3)
    for (n <- top3Number) println(n)
  }

  def saveAsTextFile(sc: SparkContext) = {
    val doubleNumbers = sc.parallelize(Range(1, 11))
      .map(_ * 2)
    val file = new File("D:\\sparkTestFile\\double_number")
    if(!file.exists()){
      doubleNumbers.saveAsTextFile("D:\\sparkTestFile\\double_number")
    }
  }


  def countByKey(sc: SparkContext) = {
    val arrStudent = Array (
      ("class1", "leo"),
      ("class2", "jack"),
      ("class1", "marry"),
      ("class2", "tom"),
      ("class2", "david")
    )
    sc.parallelize(arrStudent)
      .countByKey()
      .foreach(println)
  }
}
