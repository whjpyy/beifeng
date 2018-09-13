package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 通用的load和save操作
 * time: 2018-09-10 22:35
 */
public class GenericLoadSave {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("GenericLoadSave")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame userDf = sqlContext.read().load("hdfs://spark1:9000/spark-sql/source/users.parquet");

        userDf.printSchema();
        userDf.show();

        userDf.select("name", "favorite_color").write()
                .save("hdfs://spark1:9000/spark-sql/output/java/namesAndFavColors.parquet");
    }

}
