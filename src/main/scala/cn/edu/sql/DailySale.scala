package cn.edu.sql

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions._

/**
  * Created by Ocean lin on 2017/11/26.
  */
object DailySale {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("DailySale").setMaster("local"))
    val sQLContext = new SQLContext(sc)

    // 要使用Spark SQL的内置函数，就必须在这里导入SQLContext下的隐式转换
    import sQLContext.implicits._

    // 模拟数据
    val userSaleLog = Array("2015-10-01,55.05,1122",
      "2015-10-01,23.15,1133",
      "2015-10-01,15.20,",
      "2015-10-02,56.05,1144",
      "2015-10-02,78.87,1155",
      "2015-10-02,113.02,1123")

    val userSaleLogRDD = sc.parallelize(userSaleLog, 3)

    val userSal = userSaleLogRDD.filter(e => if (e.split(",").length == 3) true else false).
      map(e => {
        val line = e.split(",")
        Row(line(0), line(1).toDouble)
      })

    val st = StructType(Array(StructField("date", StringType, true),
      StructField("saleSum", DoubleType, true)))

    val userSaleLogDF = sQLContext.createDataFrame(userSal, st)

    userSaleLogDF.groupBy("date").agg('date, sum('saleSum)).map(e => Row(e(1), e(2)))
      .collect().foreach(println(_))

  }
}
