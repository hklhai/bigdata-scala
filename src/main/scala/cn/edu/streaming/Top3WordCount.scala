package cn.edu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ocean lin on 2017/11/29.
  */
object Top3WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Top3WordCount")
    val ssc = new StreamingContext(conf, Seconds(1))

    val products = ssc.socketTextStream("spark01", 9999)
    val windRDD = products.map(e => ((e.split(" ")(2)) + "_" + e.split(" ")(1), 1)).
      reduceByKeyAndWindow((v1: Int, v2: Int) => {
        v1 + v2
      }, Seconds(60), Seconds(10))

    windRDD.foreachRDD(e => {
      val reverseRDD = e.map(tuple => {
        val catagory = tuple._1.split("_")(0)
        val product = tuple._1.split("_")(1)
        val productcount = tuple._2
        Row(catagory, product, productcount)
      })

      val st = StructType(Array(
        StructField("catagory", StringType, true),
        StructField("product", StringType, true),
        StructField("productcount", IntegerType, true)))
      val hiveContext = new HiveContext(e.context)
      val rowDF = hiveContext.createDataFrame(reverseRDD, st)
      rowDF.registerTempTable("products")
      val top3DF = hiveContext.sql("select catagory,product,productcount from (" +
        "select catagory,product,productcount," +
        "ROW_NUMBER() OVER (PARTITION BY catagory order by productcount desc) rank from products " +
        ") tmp where rank<=3")
      top3DF.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
