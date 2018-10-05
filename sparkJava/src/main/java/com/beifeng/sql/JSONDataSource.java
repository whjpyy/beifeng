package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * JSON数据源
 * time: 2018-09-16 11:08
 */
public class JSONDataSource {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JSONDataSource")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 针对json文件，创建DataFrame
        DataFrame dfStudentScores = sqlContext.read().json("hdfs://spark1:9000/spark-sql/source/student_scores.json");
        dfStudentScores.registerTempTable("student_scores");
        DataFrame dfGoodStudentScores = sqlContext.sql("select name,score from student_scores where score >= 80");

        List<String> goodStudentNames = dfGoodStudentScores.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        List<String> studentInfoJSONs = new ArrayList<String>();
        studentInfoJSONs.add("{\"name\":\"Leo\", \"age\":18}");
        studentInfoJSONs.add("{\"name\":\"Marry\", \"age\":17}");
        studentInfoJSONs.add("{\"name\":\"Jack\", \"age\":19}");
        JavaRDD<String> rddStudentInfo = sc.parallelize(studentInfoJSONs);
        DataFrame dfStudentInfo = sqlContext.read().json(rddStudentInfo);

        dfStudentInfo.registerTempTable("student_infos");

        String sql = "select name, age from student_infos where name in (";
        for(int i = 0;i < goodStudentNames.size();i ++){
            sql += "'" + goodStudentNames.get(i) + "'";
            if(i < goodStudentNames.size() - 1){
                sql += ",";
            }
        }
        sql += ")";

        DataFrame dfGoodStudentInfo = sqlContext.sql(sql);
        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStudentRDD = dfGoodStudentScores.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), (int)row.getLong(1));
            }
        }).join(dfGoodStudentInfo.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), (int)row.getLong(1));
            }
        }));

        // 封装在RDD中的好学生的全部信息，转换为一个JavaRDD<Row>的格式
        JavaRDD<Row> goodStudentRowsRDD = goodStudentRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._1, t._2._2);
            }
        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame goodStudentsDF = sqlContext.createDataFrame(goodStudentRowsRDD, structType);
        goodStudentsDF.printSchema();
        goodStudentsDF.show();

        goodStudentsDF.save("hdfs://spark1:9000/spark-sql/output/java/good-students", "json", SaveMode.Overwrite);
    }
}
