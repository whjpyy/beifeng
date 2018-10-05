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
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;
import static org.apache.spark.sql.functions.shiftLeft;

public class RowNumberWindowFunction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("RowNumberWindowFunction")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext hiveContext = new HiveContext(sc.sc());

        JavaRDD<Row> rddSaleRows = sc.textFile("D:\\SparkTestFile\\sales.txt")
                .map(new Function<String, Row>() {
                    @Override
                    public Row call(String v1) throws Exception {
                        String arr[] = v1.split("\u0001");
                        return RowFactory.create(arr[0], arr[1], Integer.valueOf(arr[2]));
                    }
                });
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("product", DataTypes.StringType, true),
                DataTypes.createStructField("category", DataTypes.StringType, true),
                DataTypes.createStructField("revenue", DataTypes.IntegerType, true)
        );
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame dfSales = hiveContext.createDataFrame(rddSaleRows, structType);
        dfSales.show();
        dfSales.registerTempTable("sales");

        DataFrame dfTop3Sales = hiveContext.sql(
                "select product,category,revenue" +
                        " from (" +
                        "   select product, category, revenue, " +
                        "   row_number() over (partition by category order by revenue desc) rank " +
                        "   from sales" +
                        ") tmp_sales " +
                        " where rank <= 3");
        dfTop3Sales.show();

    }
}
