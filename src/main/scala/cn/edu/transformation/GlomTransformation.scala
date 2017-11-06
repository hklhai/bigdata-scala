package cn.edu.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/11/5.
  */
object GlomTransformation {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setAppName("GlomTransformation").setMaster("local"))
    val g = sc.parallelize(1 to 100, 3)
    g.glom().collect()
//    println("============")
//    g.glom().foreach(e => println(e))
  }

}
