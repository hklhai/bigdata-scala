package cn.edu.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Created by Ocean lin on 2017/11/26.
  */
object DailyUV {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("DailyUV").setMaster("local"))
    val sQLContext = new SQLContext(sc)

    // 要使用Spark SQL的内置函数，就必须在这里导入SQLContext下的隐式转换
    import sQLContext.implicits._

    val userLog = Array(
      "2015-10-01,1122",
      "2015-10-01,1122",
      "2015-10-01,1123",
      "2015-10-01,1124",
      "2015-10-01,1124",
      "2015-10-02,1122",
      "2015-10-02,1121",
      "2015-10-02,1123",
      "2015-10-02,1123")

    val userLogRDD = sc.parallelize(userLog, 5)

    // 将模拟出来的用户访问日志RDD，转换为DataFrame
    // 首先，将普通的RDD，转换为元素为Row的RDD
    val x = userLogRDD.map(e => {
      val line = e.split(",")
      Row(line(0), line(1).toInt)
    })

    // 构造DataFrame的元数据
    val st = StructType(Array(
      StructField("date", StringType, true),
      StructField("name", IntegerType, true)))
    val userLogDF = sQLContext.createDataFrame(x, st)

    // uv的基本含义和业务
    // 每天都有很多用户来访问，但每个用户可能每天都会访问很多次
    // 所以，uv指的是，对用户进行去重以后的访问总数

    // 开始使用Spark 1.5.x版本提供的最新特性，内置函数，countDistinct
    // 聚合函数的用法
    // 首先，对DataFrame调用groupBy()方法，对某一列进行分组
    // 然后，调用agg()方法 ，第一个参数，必须传入之前在groupBy()方法中出现的字段
    // 第二个参数，传入countDistinct、sum、first等，Spark提供的内置函数
    // 内置函数中，传入的参数，也是用单引号作为前缀的，其他的字段
    userLogDF.groupBy("date").agg('date, countDistinct('name)).map(e => Row(e(0), e(2))).// 第一二列都为日期，第三列为count
      collect().foreach(println(_))

  }
}
