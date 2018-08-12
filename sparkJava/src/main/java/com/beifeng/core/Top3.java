package com.beifeng.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 取最大的前3个数字
 * time: 2018-08-12 17:11
 */
public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("LocalFile")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D:\\sparkTestFile\\top.txt");

        JavaPairRDD<Integer, String> pairs = lines.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<>(Integer.valueOf(s), s);
            }
        });

        JavaPairRDD<Integer, String> sortedPairs = pairs.sortByKey(false);
        JavaRDD<Integer> sortedNumbers = sortedPairs.map(new Function<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, String> v1) throws Exception {
                return v1._1;
            }
        });

        List<Integer> top3List = sortedNumbers.take(3);
        for(Integer i : top3List){
            System.out.println(i);
        }


        sc.close();
    }
}
