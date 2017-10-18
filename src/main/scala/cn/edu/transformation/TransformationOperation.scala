package cn.edu.transformation

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/16.
  */
object TransformationOperation {

  def cogroup(): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("cogroup"))
    val studentList = Array(
      new Tuple2(1, "leo"),
      new Tuple2(2, "jack"),
      new Tuple2(3, "tom")
    )
    val scoreList = Array(
      new Tuple2(1, 100),
      new Tuple2(2, 90),
      new Tuple2(3, 60),
      new Tuple2(1, 70),
      new Tuple2(2, 80),
      new Tuple2(3, 50)
    )
    val studentRDD = sc.parallelize(studentList)
    val scoreRDD = sc.parallelize(scoreList)
    val stuInfo = studentRDD.cogroup(scoreRDD)
    stuInfo.foreach(e => println("id: " + e._1 + " name" + e._2._1 + " score:" + e._2._2))

  }

  def main(args: Array[String]): Unit = {

    //    map()

    //    filter()

    //    flatMap()

    //    groupByKey()

    //    reduceByKey()

    //    sortByKey()

    //    join()

    cogroup()
  }

  def groupByKey(): Unit = {
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val classData = Array(
      Tuple2("class1", 80),
      Tuple2("class2", 70),
      Tuple2("class1", 100),
      Tuple2("class2", 66)
    )
    val classRDD = sc.parallelize(classData)
    val groupRDD = classRDD.groupByKey()
    groupRDD.foreach(e => {
      println(e._1)
      e._2.foreach(name => println(name))
    })
  }

  def reduceByKey(): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("reduceByKey"))
    val classData = Array(
      Tuple2("class1", 80),
      Tuple2("class2", 70),
      Tuple2("class1", 100),
      Tuple2("class2", 66)
    )
    val classRDD = sc.parallelize(classData)
    classRDD.reduceByKey(_ + _).foreach(ele => println(ele._1 + ":" + ele._2))
  }

  def sortByKey(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("sortByKey").setMaster("local"))
    val numbers = Array(
      new Tuple2(65, "lee"),
      new Tuple2(70, "alex"),
      new Tuple2(97, "hk"),
      new Tuple2(87, "mary"))

    val numRDD = sc.parallelize(numbers)
    val sortRDD = numRDD.sortByKey(false)
    sortRDD.foreach(e => println(e))
  }

  def join(): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("join").setMaster("local"))
    val student = Array(
      new Tuple2(1, "leo"),
      new Tuple2(2, "jack"),
      new Tuple2(3, "tom"))
    val score = Array(
      Tuple2(1, 100),
      Tuple2(2, 90),
      Tuple2(3, 60)
    )
    val stuRDD = sc.parallelize(student)
    val scoreRDD = sc.parallelize(score)
    val stuInfo = stuRDD.join(scoreRDD)
    stuInfo.foreach(e => println("id:" + e._1 + " name:" + e._2._1 + " score: " + e._2._2))
  }


  def flatMap(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("flatMap")
    val sc = new SparkContext(conf)
    val words = Array("hello you", "hello me", "hello world")
    val wordRDD = sc.parallelize(words)
    wordRDD.flatMap(ele => ele.split(" ")).foreach(ele => println(ele))
  }


  def map(): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5)
    val unmbersRDD = sc.parallelize(numbers)
    val vRDD = unmbersRDD.map(ele => ele * 2)
    vRDD.foreach(ele => println(" val * 2 :" + ele))
  }

  def filter(): Unit = {
    val conf = new SparkConf().setAppName("filter").setMaster("local")
    val sc = new SparkContext(conf)

    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numberRDD = sc.parallelize(numbers)
    val evenRDD = numberRDD.filter(ele => ele % 2 == 0)
    evenRDD.foreach(ele => println("even number: " + ele))
  }


}
