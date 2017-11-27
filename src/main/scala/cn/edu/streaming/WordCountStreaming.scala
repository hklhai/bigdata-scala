package cn.edu.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/11/26.
  */
object WordCountStreaming {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("WordCount").setMaster("local[2]"))
    val ssc = new StreamingContext(sc, Seconds(1))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    Thread.sleep(5000)
    words.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
