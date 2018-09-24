package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("TransformationOperation")
      .setMaster("local")

    val sc = new SparkContext(conf)

    map(sc)
    filter(sc)
    flatMap(sc)
    groupByKey(sc)
    reduceByKey(sc)
    sortByKey(sc)
    join(sc)
    cogroup(sc)
  }

  def map(sc: SparkContext) = {
    sc.parallelize(Range(1, 11))
      .map(_ * 2)
      .foreach(println)
  }

  def filter(sc: SparkContext) = {
    sc.parallelize(Range(1, 11))
      .filter(n => n % 2 == 0)
      .foreach(println)
  }

  def flatMap(sc: SparkContext) = {
    val arrLine = Array("hello you", "hello me", "hello world")
    sc.parallelize(arrLine)
      .flatMap(line => line.split(" "))
      .foreach(println)
  }

  def groupByKey(sc: SparkContext) = {
    val arrScore = Array(
      ("class1", 80),
      ("class2", 75),
      ("class1", 90),
      ("class2", 65)
    )
    sc.parallelize(arrScore)
      .groupByKey()
      .foreach(println)
  }

  def reduceByKey(sc: SparkContext) = {
    val arrScore = Array(
      ("class1", 80),
      ("class2", 75),
      ("class1", 90),
      ("class2", 65)
    )
    sc.parallelize(arrScore)
      .reduceByKey(_ + _)
      .foreach(println)
  }

  def sortByKey(sc: SparkContext) = {
    val arrScore = Array(
      (65, "leo"),
      (50, "tom"),
      (100, "marry"),
      (80, "jack")
    )
    sc.parallelize(arrScore)
      .sortByKey(false)
      .foreach(println)
  }

  def join(sc: SparkContext) = {
    val arrStudent = Array(
      (1, "leo"),
      (2, "jack"),
      (3, "tom"),
      (4, "marry")
    )
    val arrScore = Array(
      (1, 100),
      (2, 90),
      (3, 60)
    )
    sc.parallelize(arrStudent)
      .join(sc.parallelize(arrScore))
      .foreach(println)
  }

  def cogroup(sc: SparkContext) = {
    val arrStudent = Array(
      (1, "leo"),
      (2, "jack"),
      (3, "tom"),
      (4, "marry")
    )
    val arrScore = Array(
      (1, 100),
      (2, 90),
      (3, 60),
      (1, 70),
      (2, 80),
      (3, 50)
    )
    sc.parallelize(arrStudent)
      .cogroup(sc.parallelize(arrScore))
      .foreach(println)
  }
}
