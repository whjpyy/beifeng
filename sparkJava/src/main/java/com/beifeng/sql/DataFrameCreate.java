package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 使用json文件创建DataFrame
 * time: 2018-09-09 17:35
 */
public class DataFrameCreate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("DataFrameCreate")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().json("hdfs://spark1:9000/spark-sql/source/students.json");

        df.show();
    }
}
