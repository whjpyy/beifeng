package com.beifeng.scala.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveDataSource {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("HiveDataSource")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("drop table if exists student_infos_scala")
    hiveContext.sql("create table if not exists student_infos_scala(name String, age int)")
    hiveContext.sql("load data local inpath '/home/spark-test/data/student_infos.txt' into table student_infos_scala")

    hiveContext.sql("drop table if exists student_scores_scala")
    hiveContext.sql("create table if not exists student_scores_scala(name String, score int)")
    hiveContext.sql("load data local inpath '/home/spark-test/data/student_scores.txt' into table student_scores_scala")

    val dfGoodStudents = hiveContext.sql("select si.name, si.age, ss.score" +
      " from student_infos_scala si" +
      " join student_scores_scala ss on ss.name = si.name" +
      " where ss.score >= 80")
    hiveContext.sql("drop table if exists good_student_infos_scala")
    dfGoodStudents.saveAsTable("good_student_infos_scala")

    hiveContext.table("good_student_infos_scala").show()
  }
}
