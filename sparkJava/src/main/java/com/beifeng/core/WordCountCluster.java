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
 * 将java开发的wordcount程序部署到spark集群上运行
 * time: 2018-08-05 15:21
 */
public class WordCountCluster {

    public static void main(String[] args) {
        // 如果要在spark集群上运行，需要修改的，只有两个地方
        // 第一，将SparkConf的setMaster方法给删掉，默认它自己会链接
        // 第二，我们针对的不是本地文件了，修改为hadoop hdfs上真正的存储大数据的文件

        // 1.创建SparkConf对象，设置Spark应用的配置信息
        SparkConf conf = new SparkConf()
                .setAppName("WordCountCluster");

        // 2.创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 3. 要针对输入源（hdfs文件，本地文件，等等），创建一个初始化的RDD
        JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/spark.txt");

        // 4.对初始RDD进行transformation操作，也就是一些计算操作

        // 先将每一行拆成单个的单词
        final JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
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

        // 到这里为止，我们通过几个spark算子操作，已经统计出了单词的次数
        worldCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordcount) throws Exception {
                System.out.println(wordcount._1 + " appeared " + wordcount._2 + " times");
            }
        });

        sc.close();
    }
}
