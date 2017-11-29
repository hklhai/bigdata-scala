package cn.edu.sql.load

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/11/25.
  */
object HiveDataSource {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("HiveDataSource"))
    val hiveContext = new HiveContext(sc)

    hiveContext.sql("drop table if exists student_info_scala")
    hiveContext.sql("create table student_info_scala (name STRING,age INT)")
    hiveContext.sql("load data " +
      "local inpath '/root/sparkstudy/file/student_infos.txt' " +
      "into table student_info_scala ")

    hiveContext.sql("drop table if exists student_scores_scala")
    hiveContext.sql("create table student_scores_scala (name STRING,score INT)")
    hiveContext.sql("load data " +
      "local inpath '/root/sparkstudy/file/student_scores.txt' " +
      "into table student_scores_scala ")

    val stu80DF = hiveContext.sql("select si.name,si.age,ss.score from student_info_scala si ,student_scores_scala ss " +
      "where si.name = ss.name and ss.score >= 80 ")

    hiveContext.sql("drop table if exists good_student_scala")
    stu80DF.saveAsTable("good_student_scala")

    val good_Student = hiveContext.table("good_student_scala").collect()
    for( e <- good_Student)
      println(e)

  }
}
