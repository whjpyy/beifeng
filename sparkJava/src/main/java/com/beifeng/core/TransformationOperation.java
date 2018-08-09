package com.beifeng.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * transformation操作实战
 * time: 2018-08-08 22:32
 */
public class TransformationOperation {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("TransformationOperation").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        map(sc);
//        filter(sc);
//        flatMap(sc);
//        groupByKey(sc);
//        reduceByKey(sc);
//        sortByKey(sc);
//        join(sc);
        cogroup(sc);
        sc.close();

    }

    /**
     * map算子实例：将集合中的每一个元素都乘以2
     *
     * @param sc
     */
    private static void map(JavaSparkContext sc) {
        List<Integer> members = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> mumberRDD = sc.parallelize(members);

        JavaRDD<Integer> MultipleMemberRDD = mumberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });

        MultipleMemberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    private static void filter(JavaSparkContext sc) {
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
        JavaRDD<Integer> filterRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer n) throws Exception {
                return n % 2 == 0;
            }
        });
        filterRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    private static void flatMap(JavaSparkContext sc) {
        List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");
        JavaRDD<String> lines = sc.parallelize(lineList);
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    private static void groupByKey(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 75),
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 65)
        );
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Iterable<Integer>> groupScores = scores.groupByKey();

        groupScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println("class:" + t._1);
                Iterator<Integer> it = t._2.iterator();
                while (it.hasNext()) {
                    System.out.println(it.next());
                }
                System.out.println("==================");
            }
        });
    }

    private static void reduceByKey(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 75),
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 65)
        );
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Integer> groupScores = scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        groupScores.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + ": " + t._2);
            }
        });
    }


    private static void sortByKey(JavaSparkContext sc) {
        List<Tuple2<Integer, String>> scoreList = Arrays.asList(
                new Tuple2<Integer, String>(65, "leo"),
                new Tuple2<Integer, String>(50, "tom"),
                new Tuple2<Integer, String>(100, "marry"),
                new Tuple2<Integer, String>(80, "jack")
        );
        JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);
        JavaPairRDD<Integer, String> groupScores = scores.sortByKey(false);

        groupScores.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._1 + ": " + t._2);
            }
        });
    }


    private static void join(JavaSparkContext sc) {
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom")
        );

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60)
        );
        
        // 并行化2个RDD
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        // 使用join算子关联两个RDD
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = students.join(scores);

        // 打印
        studentScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println("student id:" + t._1);
                System.out.println("student name: " + t._2._1);
                System.out.println("student core:" + t._2._2);
            }
        });
    }

    private static void cogroup(JavaSparkContext sc) {
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom")
        );

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(1, 70),
                new Tuple2<Integer, Integer>(2, 80),
                new Tuple2<Integer, Integer>(3, 50)
        );

        // 并行化2个RDD
        JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        // 使用join算子关联两个RDD
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores = students.cogroup(scores);

        // 打印
        studentScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println("student id:" + t._1);
                System.out.println("student name: " + t._2._1);
                System.out.println("student core:" + t._2._2);
            }
        });
    }
}
