package com.beifeng.scala.sql

import java.sql.{Connection, DriverManager, Statement}

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object JDBCDataSource {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("JDBCDataSource")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read.json("hdfs://spark1:9000/spark-sql/source/students.json")
//
    var options = scala.collection.mutable.Map("url" -> "jdbc:mysql://spark1:3306/testdb", "dbtable" -> "student_infos")
    val dfStudentInfos = sqlContext.read.format("jdbc").options(options).load()

    options("dbtable") = "student_scores"
    val dfStudentScores = sqlContext.read.format("jdbc").options(options).load()

    val rddStudentScores = dfStudentScores.rdd.map(row => (row.getString(0), row.getInt(1)))
    val rddFilterdStudents = dfStudentInfos.rdd.map(row => (row.getString(0), row.getInt(1)))
      .join(rddStudentScores)
      .map(t => Row(t._1, t._2._1, t._2._2))
      .filter(row => row.getInt(2) > 80)

    val structType = StructType(Array(
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("score", IntegerType, true)
    ))
    val dfStudents = sqlContext.createDataFrame(rddFilterdStudents, structType)
    dfStudents.show()

    // 保存到数据库中
    dfStudents.rdd.foreach(row => {
      val sql = "replace into good_student_infos values(" +
        "'" + row.getString(0) + "', " +
        "'" + row.getInt(1) + "', " +
        "'" + row.getInt(2) + "'" +
        ")";

      Class.forName("com.mysql.jdbc.Driver")

      val conn = DriverManager.getConnection("jdbc:mysql://spark1:3306/testdb")
      val stmt = conn.createStatement()
      stmt.executeUpdate(sql)
      stmt.close()
      conn.close()
    })

  }
}
