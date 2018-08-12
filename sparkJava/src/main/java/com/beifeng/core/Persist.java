package com.beifeng.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD持久化
 * time: 2018-08-12 13:19
 */
public class Persist {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Persist")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D:\\spark.txt").cache();
        long beginTime = System.currentTimeMillis();
        long count = lines.count();
        System.out.println(count);
        long endTime = System.currentTimeMillis();
        System.out.println("cost " + (endTime - beginTime) + " milliseconds");


        beginTime = System.currentTimeMillis();
        count = lines.count();
        System.out.println(count);
        endTime = System.currentTimeMillis();
        System.out.println("cost " + (endTime - beginTime) + " milliseconds");

        // cost 395 milliseconds cost 87 milliseconds no cache
        // cost 447 milliseconds cost 28 milliseconds cache
    }
}
