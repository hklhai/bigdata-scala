package cn.edu.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 如果要用scala开发spark程序
  * 然后在其中，还要实现基于反射的RDD到DataFrame的转换，就必须得用object extends App的方式
  * 不能用def main()方法的方式，来运行程序，否则就会报no typetag for ...class的错误
  * Created by Ocean lin on 2017/11/22.
  */
object RDD2DataFrameReflect extends App {

  val sc = new SparkContext(
    new SparkConf().setMaster("local").setAppName("RDD2DataFrameReflect"))

  val sQLContext = new SQLContext(sc)

  case class Student(id: Int, name: String, age: Int)


  val students = sc.textFile("D://spark//students.txt")
    .map(_.split(",")).map(arr => Student(arr(0).trim.toInt, arr(1), arr(2).trim.toInt))

  // 在Scala中使用反射方式，进行RDD到dataFrame的转换，需要手动导入一个隐式转换
  import sQLContext.implicits._

  // 使用RDD的toDF即可
  val studentDF = students.toDF()

  studentDF.registerTempTable("students")

  val teenagerDF = sQLContext.sql("select * from students where age <= 18")
  teenagerDF.show()
  println("==================Dataframe end=================")

  // 在scala中，row中的数据的顺序，反而是按照我们期望的来排列的，这个跟java是不一样
  // 再次转换为RDD
  teenagerDF.rdd.map(row =>
    Student(row(0).toString.toInt, row(1).toString, row(2).toString.toInt)).collect().
    foreach(e => println(e.id + ":" + e.name + ":" + e.age))

  println("==================RDD array end=================")

  // 在Scala中，对row的使用比Java更加丰富
  // 可以使用row的getAs方法获取指定列名的列
  teenagerDF.rdd.map(row =>
    Student(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age"))).
    collect().foreach(e => println(e.id + ":" + e.name + ":" + e.age))

  println("==================RDD getAs end=================")

  // 还可以通过row的 getValuesMap方法获取指定几列的值返回的是map
  teenagerDF.rdd.map(row => {
    val map = row.getValuesMap[Any](Array("id", "name", "age"))
    Student(map("id").toString.toInt, map("name").toString, map("age").toString.toInt)
  }).collect().foreach(e => println(e.id + ":" + e.name + ":" + e.age))
  println("==================RDD getValuesMap end=================")
}
