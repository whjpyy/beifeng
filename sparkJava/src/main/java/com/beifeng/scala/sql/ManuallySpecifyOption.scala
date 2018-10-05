package com.beifeng.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ManuallySpecifyOption {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ManuallySpecifyOption")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.format("json").load("hdfs://spark1:9000/spark-sql/source/people.json")

    df.show()
    df.printSchema()

    df.select("name").write.format("parquet").save("hdfs://spark1:9000/spark-sql/output/scala/peopleName.parquet")


  }
}
