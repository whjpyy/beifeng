package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 以编程方式动态指定元数据，将RDD转换为DataFrame
 * time: 2018-09-10 21:27
 */
public class RDD2DataFrameProgrammatically {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RDD2DataFrameProgrammatically")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> lines = sc.textFile("D:\\sparkTestFile\\students.txt");

        JavaRDD<Row> studentRDD = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                String[] lineSplited = line.split(",");
                return RowFactory.create(Integer.valueOf(lineSplited[0]), lineSplited[1], Integer.valueOf(lineSplited[2]));
            }
        });

        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        StructType structType = DataTypes.createStructType(fields);

        DataFrame studentDF = sqlContext.createDataFrame(studentRDD, structType);
        
        studentDF.registerTempTable("students");

        DataFrame teenagerDF = sqlContext.sql("select * from students where age <= 18");

        List<Row> rows = teenagerDF.javaRDD().collect();
        for(Row row : rows){
            System.out.println(row);
        }


    }
}
