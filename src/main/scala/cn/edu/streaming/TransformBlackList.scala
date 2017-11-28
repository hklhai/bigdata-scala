package cn.edu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ocean lin on 2017/11/28.
  */
// TODO: 2017/11/28 输出为字符串的位置，而不是字符串的值
object TransformBlackList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TransformBlackList")
    val ssc = new StreamingContext(conf, Seconds(5))

    val blackList = Array(("tom", true))
    val blackListRDD = ssc.sparkContext.parallelize(blackList, 5)

    val userClickDStream = ssc.socketTextStream("spark01", 9999)

    val useClick = userClickDStream.map(e => {
      val line = e.split(" ")
      (line(1), line)
    })

    val logRDD = useClick.transform(e => {
      val joinRDD = e.leftOuterJoin(blackListRDD)
      val filter = joinRDD.filter(e => {
        println(e._2._2.getOrElse(false))
        if (e._2._2.getOrElse(false)) false else true // 存在值就表示在黑名单中
      })
      val validRDD = filter.map(e => {
        println(e._2._1)
        e._2._1
      }
      )
      validRDD
    })

    logRDD.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
