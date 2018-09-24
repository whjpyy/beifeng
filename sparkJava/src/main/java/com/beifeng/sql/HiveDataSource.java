package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Hive数据源
 * time: 2018-09-16 21:41
 */
public class HiveDataSource {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("HiveDataSource");

        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        hiveContext.sql("drop table if exists student_infos");
        hiveContext.sql("create table if not exists student_infos(name String, age int)");
        hiveContext.sql("load data local inpath '/home/spark-test/data/student_infos.txt' into table student_infos");

        hiveContext.sql("drop table if exists student_scores");
        hiveContext.sql("create table if not exists student_scores(name String, score int)");
        hiveContext.sql("load data local inpath '/home/spark-test/data/student_scores.txt' into table student_scores");


        DataFrame dfGoodStudents = hiveContext.sql("select si.name, si.age, ss.score" +
                " from student_infos si " +
                " join student_scores ss on ss.name = si.name" +
                " where ss.score >= 80");

        hiveContext.sql("drop table if exists good_student_infos");
        dfGoodStudents.saveAsTable("good_student_infos");

        Row[] rowGoodStudents = hiveContext.table("good_student_infos").collect();
        for(Row goodStudent: rowGoodStudents){
            System.out.println(goodStudent);
        }
    }
}
