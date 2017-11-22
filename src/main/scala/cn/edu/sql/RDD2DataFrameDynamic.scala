package cn.edu.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * Created by Ocean lin on 2017/11/22.
  */
object RDD2DataFrameDynamic extends App {

  val sc = new SparkContext(
    new SparkConf().setMaster("local").setAppName("RDD2DataFrameDynamic"))

  val sQLContext = new SQLContext(sc)

  // 构造出元素为Row的普通RDD
  val students = sc.textFile("D://spark//students.txt").
    map(row => {
      val r = row.split(",")
      Row(r(0).toInt, r(1), r(2).toInt)
    })

  // 编程方式动态构造元数据
  val structType = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)))

  val studentsDF = sQLContext.createDataFrame(students, structType)

  studentsDF.registerTempTable("students")
  val teen = sQLContext.sql("select * from students where age <= 18")
  teen.show()

  teen.rdd.foreach(e => println(e))
}
