package com.beifeng.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 并行化集合创建RDD
 * time: 2018-08-06 23:23
 */
public class ParallelizeCollection {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ParallelizeCollection")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 通过并行化集合方式创建RDD
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

        // 执行reduce算子操作
        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });

        System.out.println("1到10的累加和：" + sum);

        sc.close();
    }
}
