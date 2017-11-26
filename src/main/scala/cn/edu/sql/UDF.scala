package cn.edu.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by Ocean lin on 2017/11/26.
  */
object UDF {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(
      new SparkConf().setMaster("local").setAppName("UDF"))

    val sQLContext = new SQLContext(sc)

    // 构造模拟数据
    val names = Array("Leo", "Marry", "Jack", "Tom")
    val namesRDD = sc.parallelize(names, 5).map(e => Row(e))
    val st = StructType(Array(StructField("name", StringType, true)))
    val nameDF = sQLContext.createDataFrame(namesRDD, st)

    // 注册一张names表
    nameDF.registerTempTable("names")

    // 定义和注册自定义函数
    // 定义函数：自己写匿名函数
    // 注册函数：SQLContext.udf.register()
    sQLContext.udf.register("strLen", (str: String) => str.length)
    sQLContext.sql("select name,strLen(name) from names").collect().foreach(println)
  }
}
