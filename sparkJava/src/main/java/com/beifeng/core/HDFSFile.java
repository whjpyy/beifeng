package com.beifeng.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 使用HDFS创建RDD
 * 案例：统计文本文件字数
 * time: 2018-08-06 23:35
 */
public class HDFSFile {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("HDFSFile")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/spark.txt");

        // 统计文本文件的字数
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        int count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("文本的字数: " + count);
    }

}
