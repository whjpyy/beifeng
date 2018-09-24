package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by youzeng
 * time: 2018-09-17 21:22
 */
public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JDBCDataSource")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://spark1:3306/testdb");
        options.put("dbtable", "student_infos");
        DataFrame dfStudentInfos = sqlContext.read().format("jdbc").options(options).load();
        dfStudentInfos.printSchema();
        dfStudentInfos.show();
    }
}
