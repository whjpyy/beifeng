package com.beifeng.scala.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object UDAF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("UDAF")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rddNameRows = sc.parallelize(Array("leo", "Marry", "Jack", "Tom", "Tom", "Tom", "leo"))
      .map(s => Row(s))
    val structType = StructType(Array(
      StructField("name", StringType, true)
    ))
    val dfNames = sqlContext.createDataFrame(rddNameRows, structType)
    dfNames.registerTempTable("names")

    sqlContext.udf.register("strCount", new StringCount)

    sqlContext.sql("select name, strCount(name) from names group by name").show()
  }
}
