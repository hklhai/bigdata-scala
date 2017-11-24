package cn.edu.sql.parquet

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/11/23.
  */
object ParquetLoad {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("ParquetLoad"))
    val sQLContext = new SQLContext(sc)
    val userDF = sQLContext.read.parquet("hdfs://spark01:9000/sql-load/users.parquet")
    userDF.registerTempTable("users")
    sQLContext.sql("select name from users")
    println("============sql=================")

    userDF.rdd.map(e => e(0)).collect().foreach(e => println("Name: " + e))
    println("============rdd=================")
  }
}
