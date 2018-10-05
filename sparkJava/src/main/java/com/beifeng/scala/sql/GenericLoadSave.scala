package com.beifeng.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object GenericLoadSave {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GenericLoadSave")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.load("hdfs://spark1:9000/spark-sql/source/users.parquet")

    df.show()
    df.printSchema()

    df.select("name", "favorite_color").write.save("hdfs://spark1:9000/spark-sql/output/scala/namesAndFavColors.parquet")


  }
}
