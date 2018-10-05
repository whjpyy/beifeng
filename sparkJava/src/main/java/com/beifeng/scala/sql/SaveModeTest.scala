package com.beifeng.scala.sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object SaveModeTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SaveModeTest")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("json").load("hdfs://spark1:9000/spark-sql/source/people.json")

    df.show()
    df.printSchema()

    df.save("hdfs://spark1:9000/spark-sql/output/scala/people_savemode_test", "json", SaveMode.Append)


  }
}
