package cn.edu.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/11/5.
  */
object GroupByTrans {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("GroupByTrans"))
    val groupby = sc.parallelize(1 to 9, 3)
    val res = groupby.groupBy(x => {
      if (x % 2 == 0) "even" else "odd"
    }).collect


  }

}
