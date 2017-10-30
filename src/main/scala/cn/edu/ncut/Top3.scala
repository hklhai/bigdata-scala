package cn.edu.ncut

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/19.
  */
object Top3 {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("Top3").setMaster("local"))
    val top = sc.textFile("D://spark//top.txt")
    top.map(e => (e.toInt,e)).sortByKey(false).take(3).foreach(e => println(e._2))
  }

}
