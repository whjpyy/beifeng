package com.beifeng.scala.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object JSONDataSource {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("JSONDataSource")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dfStudentScore = sqlContext.read.json("hdfs://spark1:9000/spark-sql/source/student_scores.json")
    dfStudentScore.registerTempTable("student_scores")
    val listGoodStudentName = sqlContext.sql("select name, score from student_scores where score >= 80").rdd.map(row => row.getString(0))
      .collect()

    val arrStudentInfoJSON = Array(
      "{\"name\":\"Leo\", \"age\":18}",
      "{\"name\":\"Marry\", \"age\":17}",
      "{\"name\":\"Jack\", \"age\":19}"
    )
    val rddStudentInfo = sc.parallelize(arrStudentInfoJSON)
    val dfStudentInfo = sqlContext.read.json(rddStudentInfo)
    dfStudentInfo.registerTempTable("student_infos")

    var sql = "select name,age from student_infos where name in ("
    for (i <- 0 until listGoodStudentName.length) {
      sql += "'" + listGoodStudentName(i) + "'"
      if (i < listGoodStudentName.length - 1) {
        sql += ","
      }
    }
    sql += ")"

    val dfGoodStudentInfos = sqlContext.sql(sql)

    val rddGoodStudentInfosRow = dfGoodStudentInfos.rdd.map ( row =>
      (row.getAs[String]("name"), row.getAs[Long]("age"))
    )

    val rddGoodStudent = dfStudentScore.rdd.map(row => {
      (row.getAs[String]("name"), row.getAs[Long]("score"))
    })
      .join(rddGoodStudentInfosRow)

    val rddGoodStudentRows = rddGoodStudent.map(info => Row(info._1, info._2._1.toInt, info._2._2.toInt))

    val stGoodStudentRow = StructType(Array(
      StructField("name", StringType, true),
      StructField("score", IntegerType, true),
      StructField("age", IntegerType, true)
    ))

    val dfGoodStudents = sqlContext.createDataFrame(rddGoodStudentRows, stGoodStudentRow)
    dfGoodStudents.save("hdfs://spark1:9000/spark-sql/output/scala/good-students", "json", SaveMode.Overwrite)

    sqlContext.read.json("hdfs://spark1:9000/spark-sql/output/scala/good-students")
      .show()
  }
}
