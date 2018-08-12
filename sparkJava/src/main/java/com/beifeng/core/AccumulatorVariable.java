package com.beifeng.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * 累加常量
 * time: 2018-08-12 15:25
 */
public class AccumulatorVariable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Persist")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        Accumulator<Integer> sum = sc.accumulator(0);
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        numbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);
            }
        });
        System.out.println(sum.value());

        sc.close();
    }
}
