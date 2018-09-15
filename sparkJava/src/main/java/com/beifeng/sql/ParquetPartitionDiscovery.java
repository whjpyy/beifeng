package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Parquet数据源之自动推断分区
 * time: 2018-09-13 23:19
 */
public class ParquetPartitionDiscovery {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ParquetPartitionDiscovery")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame usersDF = sqlContext.read().parquet("hdfs://spark1:9000/spark-sql/source/users/gender=male/country=US/users.parquet");
        usersDF.printSchema();
        usersDF.show();
    }
}
