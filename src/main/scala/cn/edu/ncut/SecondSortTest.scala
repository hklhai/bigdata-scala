package cn.edu.ncut

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/18.
  */
object SecondSortTest {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("SecondSortTest"))
    val numRDD = sc.textFile("D://sort.txt")
    val tup = numRDD.map(e => {
      Tuple2(new SecondSortKey(e.split(" ")(0).toInt, e.split(" ")(1).toInt), e)
    })
    tup.sortByKey().map(e => e._2).foreach(e => println(e))

  }


}
