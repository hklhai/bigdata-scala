package cn.edu.ncut

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/16.
  */
object LineCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LineCount").setMaster("local")
    val sc = new SparkContext(conf)
    val textRDD = sc.textFile("D://spark//hello.txt",1)
    val lineRDD = textRDD.map( line => (line,1))
    val countRDD =  lineRDD.reduceByKey(_+_)
    countRDD.foreach( t => println(t._1+ "appears:"+t._2))


  }
}
