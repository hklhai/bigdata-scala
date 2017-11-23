package cn.edu.sql.load

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/11/23.
  */
object SaveModelHDFS {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SaveModelHDFS"))
    val sQLContext = new SQLContext(sc)
    val userDF = sQLContext.read.format("json").load("hdfs://spark01:9000/sql-load/people.json")
    userDF.save("hdfs://spark01:9000/sql-load/people_savemodel_scala", "json", SaveMode.Append)
  }
}
