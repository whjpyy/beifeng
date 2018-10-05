package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by youzeng
 * time: 2018-09-17 21:22
 */
public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JDBCDataSource")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String, String> options = new HashMap<String, String>();
        options.put("url", "jdbc:mysql://spark1:3306/testdb");
        options.put("dbtable", "student_infos");
        DataFrame dfStudentInfos = sqlContext.read().format("jdbc").options(options).load();

        options.put("dbtable", "student_scores");
        DataFrame dfStudentScores = sqlContext.read().format("jdbc").options(options).load();

        JavaPairRDD<String, Tuple2<Integer, Integer>> rddStudents = dfStudentInfos.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), row.getInt(1));
            }
        }).join(dfStudentScores.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0), row.getInt(1));
            }
        }));

        JavaRDD<Row> rddFilterdStudents = rddStudents.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> v1) throws Exception {
                return RowFactory.create(v1._1, v1._2._1, v1._2._2);
            }
        }).filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                if (row.getInt(2) > 80) {
                    return true;
                }
                return false;
            }
        });

        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        structFields.add(DataTypes.createStructField("score", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame dfStudents = sqlContext.createDataFrame(rddFilterdStudents, structType);
        dfStudents.show();

        // 保存到数据库中
        dfStudents.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                String name = row.getString(0);
                int age = row.getInt(1);
                int score = row.getInt(2);
                String sql = "replace into good_student_infos values(" +
                        "'" + name + "', " +
                        "'" + age + "', " +
                        "'" + score + "'" +
                        ")";
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = null;
                Statement stmt = null;
                try {
                    conn = DriverManager.getConnection("jdbc:mysql://spark1:3306/testdb", "", "");
                    stmt = conn.createStatement();
                    stmt.executeUpdate(sql);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (stmt != null) {
                        stmt.close();
                    }
                    if (conn != null) {
                        stmt.close();
                    }
                }
            }
        });

        sc.close();

    }
}
