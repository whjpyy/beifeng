package com.beifeng.scala.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDF {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("UDF")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rddNameRows = sc.parallelize(Array("leo", "Marry", "Jack", "Tom"))
      .map(s => Row(s))
    val structType = StructType(Array(
      StructField("name", StringType, true)
    ))
    val dfNames = sqlContext.createDataFrame(rddNameRows, structType)
    dfNames.registerTempTable("names")

    sqlContext.udf.register("strLen", (str: String) => str.length)

    sqlContext.sql("select name, strLen(name) from names").show()
  }
}
