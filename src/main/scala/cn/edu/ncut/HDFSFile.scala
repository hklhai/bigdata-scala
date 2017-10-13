package cn.edu.ncut

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/12.
  */
object HDFSFile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("HDFSFile")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://spark01:9000/spark.txt")
    val sum = lines.map(line => line.length).reduce(_ + _)
    println("Sum is :" + sum)
  }
}
