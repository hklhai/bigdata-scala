package cn.edu.ncut

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/18.
  */
object AccumulatorTest {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setMaster("local").setAppName("AccumulatorTest"))

    val nums = Array(1, 2, 3, 4, 5)
    val sum = sc.accumulator(0)

    sc.parallelize(nums).foreach(e => sum += e)
    println(sum)

  }

}
