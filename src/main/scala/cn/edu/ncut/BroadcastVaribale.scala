package cn.edu.ncut

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/18.
  */
object BroadcastVaribale {
  def main(args: Array[String]): Unit = {
    val sc =new SparkContext(
      new SparkConf().setMaster("local").setAppName("BroadcastVaribale"))
    val nums = Array(1,2,3,4,5)
    val broadcastVaribale = sc.broadcast(3)
    nums.map( e => e*broadcastVaribale.value).foreach(e => println(e))
  }
}
