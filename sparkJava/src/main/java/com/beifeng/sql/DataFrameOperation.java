package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * DataFrame的常用操作
 * time: 2018-09-09 20:38
 */
public class DataFrameOperation {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("DataFrameCreate")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().json("hdfs://spark1:9000/students.json");

        // 打印DataFrame中所有的数据
        df.show();
        // 打印DataFrame中的元数据
        df.printSchema();
        // 查询某列所有的数据
        df.select("name").show();
        // 查询某几列所有的数据，并对列进行计算
        df.select(df.col("name"), df.col("age").plus(1)).show();
        // 根据某一列的值进行过滤
        df.filter(df.col("age").gt(18)).show();
        // 根据某一列进行分组，然后进行聚合
        df.groupBy(df.col("age")).count().show();

    }
}
