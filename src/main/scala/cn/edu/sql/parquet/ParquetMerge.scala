package cn.edu.sql.parquet

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by Ocean lin on 2017/11/23.
  */
object ParquetMerge {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("ParquetMerge"))
    val sQLContext = new SQLContext(sc)

    import sQLContext.implicits._

    // 创建一个DataFrame，作为学生的基本信息，并写入一个parquet文件中
    val student = Array(("hk", 23), ("lee", 25))
    val stuDF = sc.parallelize(student, 2).toDF("name", "age")
    stuDF.save("hdfs://spark01:9000/students", "parquet", SaveMode.Append)

    // 创建第二个DataFrame，作为学生的成绩信息，并写入一个parquet文件中
    val score = Array(("mary", "A"), ("peter", "B"))
    val scoreDF = sc.parallelize(score, 2).toDF("name", "score")
    scoreDF.save("hdfs://spark01:9000/students", "parquet", SaveMode.Append)

    // 首先，第一个DataFrame和第二个DataFrame的元数据肯定是不一样的吧
    // 一个是包含了name和age两个列，一个是包含了name和score两个列
    // 所以， 这里期望的是，读取出来的表数据，自动合并两个文件的元数据，出现三个列，name、age、score

    // 用mergeSchema的方式，读取students表中的数据，进行元数据的合并
    val studentDF = sQLContext.read.option("mergeSchema","true").parquet("hdfs://spark01:9000/students")
    studentDF.printSchema()
    studentDF.show()
  }
}
