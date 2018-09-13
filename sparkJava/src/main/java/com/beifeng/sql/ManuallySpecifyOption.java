package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 通用的load和save操作
 * time: 2018-09-10 22:35
 */
public class ManuallySpecifyOption {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ManuallySpecifyOption")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame peopleDf = sqlContext.read().format("json").load("hdfs://spark1:9000/spark-sql/source/people.json");

        peopleDf.printSchema();
        peopleDf.show();

        peopleDf.select("name").write().format("parquet")
                .save("hdfs://spark1:9000/spark-sql/output/java/peopleName.parquet");
    }

}
