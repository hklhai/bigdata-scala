package cn.edu.sql.load

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by Ocean lin on 2017/11/23.
  */
object SpecifyOption {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("SpecifyOption").setMaster("local"))
    val sQLContext = new SQLContext(sc)
    val userDF = sQLContext.read.format("json").load("D://spark//dataframe//people.json")
    val nameAndAge = userDF.select(userDF.col("name"), userDF.col("age"))

    nameAndAge.show()
    nameAndAge.write.save("D://spark//dataframe//people_scala")
  }

}
