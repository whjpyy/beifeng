package com.beifeng.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 分组取top3
 * time: 2018-08-12 17:21
 */
public class GroupTop3 {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("GroupTop3")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("D:\\sparkTestFile\\score.txt");


        JavaPairRDD<String, Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<>(split[0], Integer.valueOf(split[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupPairs = pairs.groupByKey();

        JavaPairRDD<String, Iterable<Integer>> top3Scores = groupPairs.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                Integer[] top3 = new Integer[3];
                String className = t._1;

                Iterator<Integer> scores = t._2.iterator();
                while(scores.hasNext()){
                    Integer score = scores.next();
                    for(int i = 0;i < 3;i ++){
                        if(top3[i] == null){
                            top3[i] = score;
                            break;
                        }else if(score > top3[i]){
                            for(int j = 2;j > i;j --){
                                top3[j] = top3[j - 1];
                            }
                            top3[i] = score;
                            break;
                        }
                    }
                }

                return new Tuple2<String,
                        Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });

        top3Scores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class: " + t._1);
                Iterator<Integer> it = t._2.iterator();
                while(it.hasNext()){
                    Integer score = it.next();
                    System.out.println(score);
                }
                System.out.println("==========================");
            }
        });

        sc.close();
    }
}
