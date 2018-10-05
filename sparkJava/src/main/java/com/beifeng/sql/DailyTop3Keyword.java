package com.beifeng.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 每日top3热点搜索词统计案例
 */
public class DailyTop3Keyword {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("DailyTop3Keyword")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        HiveContext sqlContext = new HiveContext(sc.sc());

        Map<String, List<String>> queryParamMap = new HashMap<String, List<String>>();
        queryParamMap.put("city", Arrays.asList("beijing"));
        queryParamMap.put("platform", Arrays.asList("android"));
        queryParamMap.put("version", Arrays.asList("1.0", "1.2", "1.5", "2.0"));

        final Broadcast<Map<String, List<String>>> queryParamMapBraodcast = sc.broadcast(queryParamMap);

        JavaRDD<Row> rddDateKeywordRow = sc.textFile("D:\\SparkTestFile\\keyword.txt")
                // 使用查询参数Map广播变量，进行筛选
                .filter(new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String v1) throws Exception {
                        String[] arr = v1.split("\t");
                        String city = arr[3];
                        String platform = arr[4];
                        String version = arr[5];

                        Map<String, List<String>> queryParamMap = queryParamMapBraodcast.value();
                        List<String> cities = queryParamMap.get("city");
                        if (cities.size() > 0 && !cities.contains(city)) {
                            return false;
                        }
                        List<String> platforms = queryParamMap.get("platform");
                        if (platforms.size() > 0 && !platforms.contains(platform)) {
                            return false;
                        }
                        List<String> versions = queryParamMap.get("version");
                        if (versions.size() > 0 && !versions.contains(version)) {
                            return false;
                        }
                        return true;
                    }
                })
                // 过滤出来的原始日志，映射为(日期_搜索词，用户)的格式
                .mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] arr = s.split("\t");
                        String date = arr[0];
                        String user = arr[1];
                        String keyword = arr[2];
                        return new Tuple2<String, String>(date + "_" + keyword, user);
                    }
                })
                // 进行分组，获取每天每个搜索词，有哪些用户搜索了（没有去重）
                .groupByKey()
                // 对每天每个搜索词的搜索用户，执行去重操作，获得其uv
                .mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> dateKeywordUsers) throws Exception {
                        String dateKeyword = dateKeywordUsers._1;
                        Iterator<String> users = dateKeywordUsers._2.iterator();
                        List<String> distinctUsers = new ArrayList<String>();
                        while (users.hasNext()) {
                            String user = users.next();
                            if (!distinctUsers.contains(user)) {
                                distinctUsers.add(user);
                            }
                        }
                        long uv = distinctUsers.size();
                        return new Tuple2<String, Long>(dateKeyword, uv);
                    }
                })
                .map(new Function<Tuple2<String, Long>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Long> dateKeywordUv) throws Exception {
                        String[] arr = dateKeywordUv._1.split("_");
                        return RowFactory.create(arr[0], arr[1], dateKeywordUv._2);
                    }
                });

        // 转换成DataFrame
        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true)
                );
        StructType structType = DataTypes.createStructType(structFields);
        DataFrame dfDateKeywordUv = sqlContext.createDataFrame(rddDateKeywordRow, structType);

        dfDateKeywordUv.registerTempTable("daily_keyword_uv");
        dfDateKeywordUv.show();

        DataFrame dfDailyTop3Keyword = sqlContext.sql("select date,keyword,uv" +
                " from (" +
                "   select date, keyword, uv, " +
                "   row_number() over (partition by date order by uv desc) rank" +
                "   from daily_keyword_uv" +
                ") tmp where rank <= 3");

        dfDailyTop3Keyword.show();
        // 将dataFrame转换为RDD，然后映射，计算出每天的top3搜索词的搜索uv总数
        JavaRDD<Row> rddSorted = dfDailyTop3Keyword.javaRDD()
                .mapToPair(new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        String date = row.getString(0);
                        String keyword = row.getString(1);
                        long uv = row.getLong(2);
                        return new Tuple2<String, String>(date, keyword + "_" + uv);
                    }
                })
                .groupByKey()
                .mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> t) throws Exception {
                        String date = t._1;
                        long totalUv = 0L;
                        String dateKeywords = date;
                        Iterator<String> iKeywordUv = t._2.iterator();
                        while (iKeywordUv.hasNext()) {
                            String keywordUv = iKeywordUv.next();
                            long uv = Long.valueOf(keywordUv.split("_")[1]);
                            totalUv += uv;
                            dateKeywords += "," + keywordUv;
                        }
                        return new Tuple2<Long, String>(totalUv, dateKeywords);
                    }
                })
                .sortByKey(false)
                .flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
                    @Override
                    public Iterable<Row> call(Tuple2<Long, String> t) throws Exception {
                        String dateKeywords = t._2;
                        String[] arrDateKeyword = dateKeywords.split(",");
                        String date = arrDateKeyword[0];
                        List<Row> rows = new ArrayList<Row>();
                        rows.add(RowFactory.create(date, arrDateKeyword[1].split("_")[0], Long.valueOf(arrDateKeyword[1].split("_")[1])));
                        rows.add(RowFactory.create(date, arrDateKeyword[2].split("_")[0], Long.valueOf(arrDateKeyword[2].split("_")[1])));
                        rows.add(RowFactory.create(date, arrDateKeyword[3].split("_")[0], Long.valueOf(arrDateKeyword[3].split("_")[1])));
                        return rows;
                    }
                });

        DataFrame dfFinal = sqlContext.createDataFrame(rddSorted, structType);
        dfFinal.show();

    }
}
