package cn.edu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ocean lin on 2017/11/28.
  */
object TransformBlackList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TransformBlackList")
    val ssc = new StreamingContext(conf, Seconds(5))

    val blacklist = Array(("tom", true))
    val blacklistRDD = ssc.sparkContext.parallelize(blacklist, 5)

    val adsClickLogDStream = ssc.socketTextStream("spark01", 9999)
    val userAdsClickLogDStream = adsClickLogDStream
      .map { adsClickLog => (adsClickLog.split(" ")(1), adsClickLog) }

    val validAdsClickLogDStream = userAdsClickLogDStream.transform(userAdsClickLogRDD => {
      val joinedRDD = userAdsClickLogRDD.leftOuterJoin(blacklistRDD)
      val filteredRDD = joinedRDD.filter(tuple => {
        if (tuple._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
      val validAdsClickLogRDD = filteredRDD.map(tuple => tuple._2._1)
      val ui = validAdsClickLogRDD.collect()
      if (ui.length > 0)
        println(ui(0))
      validAdsClickLogRDD
    })
    // TODO 输出两次
    //    validAdsClickLogDStream.foreach(e => {
    //      val x = e.collect()
    //      if (x.length > 0)
    //        println(x(0))
    //    })
    validAdsClickLogDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
