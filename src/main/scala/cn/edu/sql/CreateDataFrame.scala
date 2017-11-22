package cn.edu.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/11/22.
  */
object CreateDataFrame {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("CreateDataFrame"))
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.json("hdfs://spark01:9000/students.json")
    df.show()

  }

}
