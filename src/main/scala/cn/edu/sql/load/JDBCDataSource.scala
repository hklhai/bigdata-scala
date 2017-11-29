package cn.edu.sql.load

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by Ocean lin on 2017/11/29.
  */
object JDBCDataSource {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("JDBCDataSource"))
    val sQLContext = new SQLContext(sc)

    val options = new mutable.HashMap[String, String]()
    options.put("url", "jdbc:mysql://spark01:3306/hivedb")
    options.put("dbtable", "student_infos")
    val stuentDF = sQLContext.read.format("jdbc").options(options).load()

    options.put("dbtable", "student_scores")
    val stuentScoreDF = sQLContext.read.format("jdbc").options(options).load()

    val stuentScoreRDD = stuentScoreDF.rdd.map(x => (x.get(0), x.get(1)))
    val joinRDD = stuentDF.rdd.map(e => (e.get(0), e.get(1))).join(stuentScoreRDD)
    val goodStuRDD = joinRDD.filter(e => if (e._2._2.toString.toInt > 80) true else false)
      .map(e => Row(e._1, e._2._1, e._2._2))

    val st = StructType(Array(StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("score", IntegerType, true)
    ))

    val studentScoreDF = sQLContext.createDataFrame(goodStuRDD, st).collect()
    for (e <- studentScoreDF)
      println(e)

    // 持久化
    studentScoreDF.foreach(e => {
      val sql = "insert into good_students (name,age,score) values ('" + e.get(0) + "'," + e.get(1) + "," + e.get(2) + ")";

      // Change to Your Database Config
      val conn_str = "jdbc:mysql://spark01:3306/hivedb?user=&password="
      // Load the driver
      classOf[com.mysql.jdbc.Driver]
      // Setup the connection
      val conn = DriverManager.getConnection(conn_str)
      try {
        // Configure to be Read Only
        val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
        // Execute Query
        val rs = statement.execute(sql)
      } catch {
        case e: Exception => e.printStackTrace
      }
      finally {
        conn.close
      }
    })
  }
}
