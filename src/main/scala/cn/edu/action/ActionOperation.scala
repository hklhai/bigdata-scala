package cn.edu.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/18.
  */
object ActionOperation {

  def reduce(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("").setMaster("local"))
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbersRDD = sc.parallelize(numbers)
    val resRDD = numbersRDD.reduce(_ + _)
    println(resRDD)

  }

  def main(args: Array[String]): Unit = {
    reduce()
  }
}
