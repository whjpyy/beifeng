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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.util.Arrays;
import java.util.List;
import static org.apache.spark.sql.functions.*;

public class DailySale {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("DailySale")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        List<String> userSaleLog = Arrays.asList(
                "2015-10-01,55.05,1122",
                "2015-10-01,23.15,1133",
                "2015-10-01,15.20,",
                "2015-10-02,56.05,1144",
                "2015-10-02,78.87,1155",
                "2015-10-02,113.02,1123"
        );
        JavaRDD<Row> rddUserLogRows = sc.parallelize(userSaleLog)
                .filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String v1) throws Exception {
                        return v1.split(",").length == 3;
                    }
                }).map(new Function<String, Row>() {
                    @Override
                    public Row call(String v1) throws Exception {
                        String[] arr = v1.split(",");
                        return RowFactory.create(arr[0], Double.valueOf(arr[1]));
                    }
                });

        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("sale_amount", DataTypes.DoubleType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame dfUserSaleLog = sqlContext.createDataFrame(rddUserLogRows, structType);

        dfUserSaleLog.groupBy("date")
                .agg(sum("sale_amount"), avg("sale_amount"))
                .toJavaRDD()
                .foreach(new VoidFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        System.out.println(row);
                    }
                });
    }
}
