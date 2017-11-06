package cn.edu.ncut

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/9/21.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("WordCount"))
    sc.textFile("hdfs://spark01:9000/spark.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
      .foreach(e => println(e._1 + ":" + e._2))
  }

}
