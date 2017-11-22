package cn.edu.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Ocean lin on 2017/11/22.
  */
object DataFrameOperation {

  def main(args: Array[String]): Unit = {
    val sqlContext = new SQLContext(new SparkContext(new SparkConf().setAppName("DataFrameOperation")))
    val df = sqlContext.read.json("hdfs://spark01:9000/students.json")
    df.show()

    df.printSchema()

    df.select(df.col("name")).show()

    df.select(df.col("name"), df.col("age").plus(1)).show()

    df.filter(df.col("age") > 18).show()

    df.groupBy(df.col("age")).count().show()
  }
}
