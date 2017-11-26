package cn.edu.sql.load

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/11/25.
  */
object JsonDataSource {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("JsonDataSource"))
    val sQLContext = new SQLContext(sc)

    // 针对json文件，创建DataFrame（针对json文件创建DataFrame）
    val stuDF = sQLContext.read.json("hdfs://spark01:9000/spark-study/students.json")
    stuDF.registerTempTable("stus")
    val stu80DF = sQLContext.sql("select name,score from stus where score>80")
    stu80DF.show()

    println("============stu80DF==============")

    // （将DataFrame转换为rdd，执行transformation操作）
    val stuArray = stu80DF.rdd.map(e => e.get(0)).collect()

    // （针对包含json串的JavaRDD，创建DataFrame）
    val stuInfo = Array("{\"name\":\"Leo\",\"age\":24}",
      "{\"name\":\"Marry\",\"age\":23}",
      "{\"name\":\"Jack\",\"age\":27}")

    val stuInfoRDD = sc.parallelize(stuInfo, 1)
    val stuInfoDF = sQLContext.read.json(stuInfoRDD)
    stuInfoDF.registerTempTable("stuInfo")

    var sql = "select name,age from stuInfo where name in ("
    for (i <- 0 until stuArray.length) {
      sql += "'" + stuArray(i) + "'"
      if (i < stuArray.length - 1)
        sql += ","
    }
    sql += ")"

    val students = sQLContext.sql(sql)
    val stuRDD = students.rdd.map { row => (row.getAs[String]("name"), row.getAs[Long]("age")) }

    val goodStudentsRDD =
      stu80DF.rdd.map { row => (row.getAs[String]("name"), row.getAs[Long]("score")) }.join(stuRDD)

    val studentRowRDd = goodStudentsRDD.map(e => Row(e._1, e._2._1, e._2._2))

    val st = StructType(Array(StructField("name", StringType, true),
      StructField("score", LongType, true),
      StructField("age", LongType, true)))
    val stuGoodDF = sQLContext.createDataFrame(studentRowRDd, st)
    stuGoodDF.write.format("json").save("hdfs://spark01:9000/GoodStudent-scala")

  }
}
