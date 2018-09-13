package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * Parquet数据源值使用编程方式加载数据
 * time: 2018-09-13 22:45
 */
public class ParquetLoadData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ParquetLoadData")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame usersDF = sqlContext.read().parquet("hdfs://spark1:9000/spark-sql/source/users.parquet");
        
        usersDF.registerTempTable("users");
        DataFrame userNamesDF = sqlContext.sql("select name from users");

        List<String> userNames = userNamesDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "Name: " + row.getString(0);
            }
        }).collect();

        for (String userName: userNames){
            System.out.println(userName);
        }

    }
}
