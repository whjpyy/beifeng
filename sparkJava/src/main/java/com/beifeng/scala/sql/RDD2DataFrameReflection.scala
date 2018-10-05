package com.beifeng.scala.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RDD2DataFrameReflection {
  case class Student(id: Int, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDD2DataFrameReflection")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val dfStudent = sc.textFile("D:\\sparkTestFile\\students.txt")
      .map(line => {
        val arr = line.split(",")
        Student(arr(0).toInt, arr(1), arr(2).toInt)
      })
      .toDF()
    dfStudent.registerTempTable("students")

    val rddTeenager = sqlContext.sql("select * from students where age <= 18").rdd
    rddTeenager.foreach(println)

    rddTeenager.map(row => Student(row.getInt(0), row.getString(1),row.getInt(2))).foreach(println)

    rddTeenager.map(row => Student(row.getAs("id"), row.getAs("name"), row.getAs("age")))
      .foreach(println)

    rddTeenager.map(row => {
      val map = row.getValuesMap[Any](Array("id", "name", "age"))
      Student(map("id").toString.toInt, map("name").toString,map("age").toString.toInt)
    }).foreach(println)
  }
}
