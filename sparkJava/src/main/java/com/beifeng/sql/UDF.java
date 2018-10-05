package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;

public class UDF {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("UDF")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        List<String> names = Arrays.asList("Leo", "Marry", "Jack", "Tom");
        JavaRDD<Row> rddNameRows = sc.parallelize(names, 5)
                .map(new Function<String, Row>() {
                    @Override
                    public Row call(String v1) throws Exception {
                        return RowFactory.create(v1);
                    }
                });
        StructType structType = DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("name", DataTypes.StringType, true)));
        DataFrame dfNames = sqlContext.createDataFrame(rddNameRows, structType);
        dfNames.registerTempTable("names");

        sqlContext.udf().register("strLen", new UDF1<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        }, DataTypes.IntegerType);

        sqlContext.sql("select name, strLen(name) from names")
                .show();
    }
}
