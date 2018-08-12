package com.beifeng.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Action操作实战
 * time: 2018-08-09 21:55
 */
public class ActionOperation {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ActionOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        reduce(sc);
//        collect(sc);
//        count(sc);
//        take(sc);
//        saveAsTextFile(sc);
      countByKey(sc);

        sc.close();
    }

    private static void reduce(JavaSparkContext sc){
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        Integer sum = numbers.reduce((v1, v2) -> {
            return v1 + v2;
        });

        System.out.println(sum);
    }

    private static void collect(JavaSparkContext sc){
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        JavaRDD<Integer> doubleNumber = numbers.map(n -> {
            return n * 2;
        });

        List<Integer> collectList = doubleNumber.collect();
        for(Integer num: collectList){
            System.out.println(num);
        }
    }

    private static void count(JavaSparkContext sc){
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        long count = numbers.count();
        System.out.println(count);
    }

    private static void take(JavaSparkContext sc){
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        List<Integer> top3Numbers = numbers.take(3);
        for(Integer num: top3Numbers) {
            System.out.println(num);
        }
    }

    private static void saveAsTextFile(JavaSparkContext sc){
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);

        JavaRDD<Integer> doubleNumber = numbers.map(n -> {
            return n * 2;
        });

        doubleNumber.saveAsTextFile("D:\\double_number.txt");
//        doubleNumber.saveAsTextFile("hdfs://spark1:9000/double_number.txt");
    }

    private static void countByKey(JavaSparkContext sc) {
        List<Tuple2<String, String>> studentList = Arrays.asList(
                new Tuple2<String, String>("class1", "leo"),
                new Tuple2<String, String>("class2", "jack"),
                new Tuple2<String, String>("class1", "marry"),
                new Tuple2<String, String>("class2", "tom"),
                new Tuple2<String, String>("class2", "david")
        );
        JavaPairRDD<String, String> students = sc.parallelizePairs(studentList);

        Map<String, Object> studentCounts = students.countByKey();
        for(Map.Entry<String, Object> studentCount : studentCounts.entrySet()){
            System.out.println(studentCount.getKey() + ": " + studentCount.getValue());
        }
    }
}
