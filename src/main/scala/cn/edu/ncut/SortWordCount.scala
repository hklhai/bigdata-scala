package cn.edu.ncut

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/18.
  */
object SortWordCount {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("SortWordCount").setMaster("local"))
    val lines = sc.textFile("D://spark.txt")
    lines.flatMap(e => e.split(" ")).map(e => (e, 1)).reduceByKey(_ + _).
      map(e => (e._2, e._1)).sortByKey(false).map(e => (e._2, e._1)).
      foreach(e => println(e._1 + " : " + e._2))
  }
}

