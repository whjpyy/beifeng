package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parquet数据源之合并元数据
 * time: 2018-09-13 23:19
 */
public class ParquetMergeSchema {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ParquetMergeSchema")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        // 创建一个DataFrame，作为学生的基本信息，并写入一个parquet文件中
        List<Tuple2<String, Integer>> studentsWithNameAge = Arrays.asList(new Tuple2<String, Integer>("leo", 23), new Tuple2<String, Integer>("jack", 25));

        JavaRDD<Row> studentNameAndAgeRow = sc.parallelize(studentsWithNameAge, 2).map(new Function<Tuple2<String, Integer>, Row>() {
            @Override
            public Row call(Tuple2<String, Integer> t) throws Exception {
                return RowFactory.create(t._1, t._2);
            }
        });
        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structTypeNameAndAge = DataTypes.createStructType(fields);
        DataFrame studentsWithNameAgeDF = sqlContext.createDataFrame(studentNameAndAgeRow, structTypeNameAndAge);
        studentsWithNameAgeDF.save("hdfs://spark1:9000/spark-sql/output/java/students", "parquet", SaveMode.Append);

        // 创建第二个DataFrame，作为学生的基本信息，并写入一个parquet文件中
        List<Tuple2<String, String>> studentsWithNameGrade = Arrays.asList(new Tuple2<String, String>("marry", "A"), new Tuple2<String, String>("tom", "B"));

        JavaRDD<Row> studentNameAndGradeRow = sc.parallelize(studentsWithNameGrade, 2).map(new Function<Tuple2<String, String>, Row>() {
            @Override
            public Row call(Tuple2<String, String> t) throws Exception {
                return RowFactory.create(t._1, t._2);
            }
        });
        List<StructField> fields2 = new ArrayList<StructField>();
        fields2.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields2.add(DataTypes.createStructField("grade", DataTypes.StringType, true));
        StructType structTypeNameAndGrade = DataTypes.createStructType(fields2);
        DataFrame studentsWithNameGradeDF = sqlContext.createDataFrame(studentNameAndGradeRow, structTypeNameAndGrade);
        studentsWithNameGradeDF.save("hdfs://spark1:9000/spark-sql/output/java/students", "parquet", SaveMode.Append);

        // 用mergeSchema的方式，读取students表中的数据，进行元数据的合并
        DataFrame students = sqlContext.read().option("mergeSchema", "true").parquet("hdfs://spark1:9000/spark-sql/output/java/students");
        students.printSchema();
        students.show();
    }
}
