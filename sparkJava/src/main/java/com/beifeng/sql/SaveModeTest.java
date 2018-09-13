package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * Created by youzeng
 * time: 2018-09-10 22:59
 */
public class SaveModeTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SaveModeTest")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame peopleDf = sqlContext.read().format("json").load("hdfs://spark1:9000/spark-sql/source/people.json");
        peopleDf.save("hdfs://spark1:9000/spark-sql/output/java/people_savemode_test", "json", SaveMode.Append);

    }
}
