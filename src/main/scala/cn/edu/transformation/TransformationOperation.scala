package cn.edu.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/16.
  */
object TransformationOperation {

  def main(args: Array[String]): Unit = {

    //    map()
    filter()
  }

  def map(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("TransformationOperationMap")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5)
    val unmbersRDD = sc.parallelize(numbers)
    val vRDD = unmbersRDD.map(ele => ele * 2)
    vRDD.foreach(ele => println(" val * 2 :" + ele))
  }

  def filter(): Unit = {
    val conf = new SparkConf().setAppName("TransformationOperationFilter").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers)
    val evenRDD = numberRDD.filter(ele => ele % 2 == 0)
    evenRDD.foreach(ele => println("even number: " + ele))

  }

}
