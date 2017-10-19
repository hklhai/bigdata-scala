package cn.edu.ncut

import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Ocean lin on 2017/10/19.
  */
object GroupTop3 {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("GroupTop3"))
    val score = sc.textFile("D://spark//score.txt")
    score.map(e => (e.split(" ")(0), (e.split(" ")(1).toInt)))
      .groupByKey().map(e => {
      var result = List[Int]()
      for (element <- e._2) {
        result = result ::: List(element)
      }
      val sort = result.sorted(Ordering.Int.reverse)
      var top3 = List[Int]()
      for (i <- 0 to 2) {
        top3 = top3 ::: List(sort(i))
      }
      (e._1, top3)
    }).foreach({ e => println(e._1); e._2.foreach(x => println(x))
    }
    )
  }

}
