package cn.edu.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ocean lin on 2017/11/26.
  */
object HDFSWordCount {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("HDFSWordCount").setMaster("local[2]"))
    val ssc = new StreamingContext(sc, Seconds(5))

    val lines = ssc.textFileStream("hdfs://spark01:9000/streaming")
    val wordCount = lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

    wordCount.print()


    ssc.start()
    ssc.awaitTermination()
  }

}
