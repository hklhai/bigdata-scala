package cn.edu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Ocean lin on 2017/11/27.
  */
object UpdateStateByKeyWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("UpdateStateByKeyWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    // 检查点路径不可以公用
    ssc.checkpoint("hdfs://spark01:9000/wordcount_checkpoint_scala")
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" ")).map((_, 1))
    words.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      var newValue = state.getOrElse(0)
      for (e <- values)
        newValue += e
      Option(newValue)
    }).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
