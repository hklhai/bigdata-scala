package cn.edu.action

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/18.
  */
object ActionOperation {

  def reduce(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("reduce").setMaster("local"))
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbersRDD = sc.parallelize(numbers)
    val resRDD = numbersRDD.reduce(_ + _)
    println(resRDD)
  }

  def collect(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("collect").setMaster("local"))
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbersRDD = sc.parallelize(numbers)
    val resRDD = numbersRDD.collect()
    resRDD.foreach(e => println(e))
  }

  def count() = {
    val sc = new SparkContext(new SparkConf().setAppName("count").setMaster("local"))
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbersRDD = sc.parallelize(numbers)
    val sum = numbersRDD.count()
    println(sum)
  }

  def take(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("take").setMaster("local"))
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbersRDD = sc.parallelize(numbers)
    val resRDD = numbersRDD.take(6)
    resRDD.foreach(e => println(e))
  }

  def saveAsTextFile(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("saveAsTextFile").setMaster("local"))
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbersRDD = sc.parallelize(numbers)
    numbersRDD.saveAsTextFile("hdfs://spark01:9000/scala_numbers")
  }

  def countByKey(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("saveAsTextFile").setMaster("local"))
    val numbers = Array(
      Tuple2("class1", "leo"),
      Tuple2("class2", "jack"),
      Tuple2("class1", "marry"),
      Tuple2("class2", "tom"),
      Tuple2("class2", "david"));
    val numRDD = sc.parallelize(numbers)
    val res = numRDD.countByKey()
    println(res)
  }

  def main(args: Array[String]): Unit = {
    //    reduce()

    //    collect()

    //    count()

    //    take()

//    saveAsTextFile()

        countByKey()
  }
}
