package cn.edu.sql.load

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/11/23.
  */
object SpecifyOptionHDFS {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SpecifyOptionHDFS"))
    val sQLContext = new SQLContext(sc)
    val userDF = sQLContext.read.format("json").load("hdfs://spark01:9000/sql-load/people.json")
    val nameAndAge = userDF.select(userDF.col("name"), userDF.col("age"))

    nameAndAge.show()
    nameAndAge.write.save("hdfs://spark01:9000/sql-load/people_scala")
  }

}
