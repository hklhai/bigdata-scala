package cn.edu.sql.load

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HDFS上通用的load & save
  * Created by Ocean lin on 2017/11/23.
  */
object GenericLoadSave4HDFS {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("GenericLoadSave4HDFS"))
    val sQLContext = new SQLContext(sc)
    val userDF = sQLContext.read.load("hdfs://spark01:9000/sql-load/users.parquet")
    userDF.show()
    userDF.write.save("hdfs://spark01:9000/sql-load/users_scala")
  }
}
