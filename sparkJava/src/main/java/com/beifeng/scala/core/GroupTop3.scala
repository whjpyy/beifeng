package com.beifeng.scala.core

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._

object GroupTop3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("GroupTop3")
      .setMaster("local")
    val sc = new SparkContext(conf)

    sc.textFile("D:\\sparkTestFile\\score.txt")
      .map(line => {
        val arr = line.split(" ")
        (arr(0), arr(1).toInt)
      })
      .groupByKey()
      .map(t => {
        val top3 = new Array[Int](3)
        val className = t._1
        val scores = t._2.iterator
        while(scores.hasNext){
          val score = scores.next()
          breakable{
            for(i <- 0 until 3){
              if(top3(i) == Nil){
                top3(i) = score
                break;
              }else{
                var j = 2
                while(j > i){
                  top3(j) = top3(j - 1)
                  j -= 1
                }
                top3(i) = score
                break;
              }
            }
          }
        }
        (className, top3)
      })
      .foreach(t => {
        println("class: " + t._1)
        for(i <- 0 until t._2.length) println(t._2(i))
        println("======================")
      })
  }
}
