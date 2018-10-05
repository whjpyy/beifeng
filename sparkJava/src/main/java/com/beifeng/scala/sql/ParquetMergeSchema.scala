package com.beifeng.scala.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object ParquetMergeSchema {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ParquetMergeSchema")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val arrStudentWithNameAge = Array(
      ("leo", 23),
      ("jack", 25)
    )

    val rddStudentNameAge = sc.parallelize(arrStudentWithNameAge)
      .map(t => Row(t._1, t._2))
    val stStudentNameAge = StructType(Array(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true)
    ))
    val dfStudentNameAge = sqlContext.createDataFrame(rddStudentNameAge, stStudentNameAge)
    dfStudentNameAge.save("hdfs://spark1:9000/spark-sql/output/scala/students", "parquet", SaveMode.Append)

    val arrStudentWithNameGrade = Array(
      ("marry", "A"),
      ("tom", "B")
    )
    val rddStudentNameGrade = sc.parallelize(arrStudentWithNameGrade)
      .map(t => Row(t._1, t._2))
    val stStudentNameGrade = StructType(Array(
      StructField("name", StringType, true),
      StructField("grade", StringType, true)
    ))
    val dfStudentNameGrade = sqlContext.createDataFrame(rddStudentNameGrade, stStudentNameGrade)
    dfStudentNameGrade.save("hdfs://spark1:9000/spark-sql/output/scala/students", "parquet", SaveMode.Append)

    val dfStudent = sqlContext.read.option("mergeSchema", "true").parquet("hdfs://spark1:9000/spark-sql/output/scala/students")
    dfStudent.show()
    dfStudent.printSchema()

  }
}
