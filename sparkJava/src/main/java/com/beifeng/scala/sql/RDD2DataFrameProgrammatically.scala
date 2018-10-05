package com.beifeng.scala.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object RDD2DataFrameProgrammatically {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDD2DataFrameProgrammatically")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)


    val rddStudent = sc.textFile("D:\\sparkTestFile\\students.txt")
      .map(line => {
        val arr = line.split(",")
        Row(arr(0).toInt, arr(1), arr(2).toInt)
      })

    val structType = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))

    val dfStudent = sqlContext.createDataFrame(rddStudent, structType)

    dfStudent.registerTempTable("students")

    val rddTeenager = sqlContext.sql("select * from students where age <= 18").rdd
    rddTeenager.foreach(println)
  }
}
