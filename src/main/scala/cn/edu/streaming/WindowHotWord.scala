package cn.edu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ocean lin on 2017/11/28.
  */
object WindowHotWord {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TransformBlacklist")
    val ssc = new StreamingContext(conf, Seconds(5))

    val userSearchLog = ssc.socketTextStream("spark01", 9999)
    val words = userSearchLog.map(_.split(" ")(1)).map((_, 1))

    val windowsDStream = words.reduceByKeyAndWindow(
      (v1: Int, v2: Int) => {
        v1 + v2
      }, Seconds(60), Seconds(10))

    val sRDD = windowsDStream.transform(e => {
      val reverse = e.map(x => (x._2, x._1))
      val sort = reverse.sortByKey(false)
      val sortWordCount = sort.map(u => (u._2, u._1))
      val top3 = sortWordCount.take(3)
      for (x <- top3)
        println(x)
      e //随便返回一个数据即可
    })
    sRDD.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
