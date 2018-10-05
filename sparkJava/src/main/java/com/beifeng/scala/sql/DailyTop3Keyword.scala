package com.beifeng.scala.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object DailyTop3Keyword {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DailyTop3Keyword")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)


    val queryParamMap = Map("city" -> Array("beijing"), "platform" -> Array("android"), "version" -> Array("1.0", "1.2", "1.5", "2.0"))
    val bcQueryParamMap = sc.broadcast(queryParamMap)


    val rddDateKeywordRow = sc.textFile("D:\\SparkTestFile\\keyword.txt")
      .filter(line => {
        val arr = line.split("\t")
        val city = arr(3)
        val platform = arr(4)
        val version = arr(5)
        val queryParamMap = bcQueryParamMap.value
        var isFilter = true

        val cities = queryParamMap("city")
        if (cities.length > 0 && !cities.contains(city)) {
          isFilter = false
        }
        val platforms = queryParamMap("platform")
        if (platforms.length > 0 && !platforms.contains(platform)) {
          isFilter = false
        }
        val versions = queryParamMap("version")
        if (versions.length > 0 && !versions.contains(version)) {
          isFilter = false
        }
        isFilter
      })

      .map(line => {
        val arr = line.split("\t")
        (arr(0) + "_" + arr(2), arr(1))
      })
      .groupByKey()
      .map(t => {
        val dateKeyword = t._1
        val users = t._2.iterator
        val distinctUsers = ArrayBuffer[String]()
        while (users.hasNext) {
          val str = users.next()
          if (!distinctUsers.contains(str)) {
            distinctUsers += str
          }
        }
        (dateKeyword, distinctUsers.length.toLong)
      })
      .map(t => {
        val arr = t._1.split("_")
        Row(arr(0), arr(1), t._2)
      })

    val structType = StructType(Array(
      StructField("date", StringType, true),
      StructField("keyword", StringType, true),
      StructField("uv", LongType, true)
    ))
    val dfDateKeywordUv = sqlContext.createDataFrame(rddDateKeywordRow, structType)
    dfDateKeywordUv.show()

    dfDateKeywordUv.registerTempTable("daily_keyword_uv")

    val dfDailyTop3Keyword = sqlContext.sql("select date,keyword,uv" +
      " from (" +
      "   select date, keyword, uv, " +
      "   row_number() over (partition by date order by uv desc) rank" +
      "   from daily_keyword_uv" +
      ") tmp where rank <= 3")
    dfDailyTop3Keyword.show()

    // 将dataFrame转换为RDD，计算出每天的top3搜索词的搜索总数uv
    val rddSorted = dfDailyTop3Keyword.rdd
      .map(row => {
        (row(0), row(1) + "_" + row(2))
      })
      .groupByKey()
      .map(t => {
        val date = t._1
        var totalUv = 0L
        var dateKeywords = date
        val iKeywordUv = t._2.iterator
        while (iKeywordUv.hasNext) {
          val keywordUv = iKeywordUv.next()
          val uv = keywordUv.split("_")(1).toLong
          totalUv += uv
          dateKeywords += "," + keywordUv
        }
        (totalUv, dateKeywords)
      })
      .sortByKey(false)
      .flatMap(t => {
        val dateKeywords = t._2.toString
        val arrDateKeyword = dateKeywords.split(",")
        val date = arrDateKeyword(0)
        val rows = Array(
          Row(date, arrDateKeyword(1).split("_")(0), arrDateKeyword(1).split("_")(1).toLong),
          Row(date, arrDateKeyword(2).split("_")(0), arrDateKeyword(2).split("_")(1).toLong),
          Row(date, arrDateKeyword(3).split("_")(0), arrDateKeyword(3).split("_")(1).toLong)
        )
        rows
      })
      sqlContext.createDataFrame(rddSorted, structType)
        .show()
  }
}
