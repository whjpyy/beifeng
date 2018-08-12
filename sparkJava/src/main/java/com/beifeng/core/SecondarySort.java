package com.beifeng.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created by youzeng
 * time: 2018-08-12 16:05
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SortWordCount")
                .setMaster("local");

        // 2.创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 要针对输入源（hdfs文件，本地文件，等等），创建一个初始化的RDD
        JavaRDD<String> lines = sc.textFile("D:\\sort.txt");

        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] lineSplited = s.split(" ");
                SecondarySortKey key = new SecondarySortKey(Integer.valueOf(lineSplited[0]), Integer.valueOf(lineSplited[1]));
                return new Tuple2<>(key, s);
            }
        });

        JavaPairRDD<SecondarySortKey, String> sortedPairs = pairs.sortByKey(false);

        JavaRDD<String> soredLines = sortedPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        soredLines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
