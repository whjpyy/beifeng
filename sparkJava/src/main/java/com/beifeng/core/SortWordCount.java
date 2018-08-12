package com.beifeng.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 排序的wordcount程序
 * time: 2018-08-05 15:21
 */
public class SortWordCount {

    public static void main(String[] args) {
        // 编写spark应用程序

        // 1.创建SparkConf对象，设置Spark应用的配置信息
        SparkConf conf = new SparkConf()
                .setAppName("SortWordCount")
                .setMaster("local");

        // 2.创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 要针对输入源（hdfs文件，本地文件，等等），创建一个初始化的RDD
        JavaRDD<String> lines = sc.textFile("D:\\spark.txt");

        // 4.对初始RDD进行transformation操作，也就是一些计算操作

        // 先将每一行拆成单个的单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        // 接着，需要将每一个单词映射为（单词，1）这种格式
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        // 接着，需要以单词作为可以，统计每个单词出线的次数
        // 使用reduceByKey,对每个key的value，都进行reduce操作
        JavaPairRDD<String, Integer> worldCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 我们需要将(hello, 3)转换为(3, hello)格式
        // 进行key-value的反转映射
        JavaPairRDD<Integer, String> countWords = worldCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2, t._1);
            }
        });

        // 按照key进行排序
        JavaPairRDD<Integer, String> sortedCountWords = countWords.sortByKey(false);

        // 在进行key-value的反转映射
        JavaPairRDD<String, Integer> sortedWordCount = sortedCountWords.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2, t._1);
            }
        });

        // 到此为止，我们获得了安装单词出现次数排序后的RDD
        sortedWordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appeared " + t._2 + " times");
            }
        });

        // 到这里为止，我们通过几个spark算子操作，已经统计出了单词的次数
//        worldCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> wordcount) throws Exception {
//                System.out.println(wordcount._1 + " appeared " + wordcount._2 + " times");
//            }
//        });

        sc.close();
    }
}
