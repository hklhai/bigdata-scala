import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Ocean lin on 2017/10/12.
  */
object ParallelizeCollection {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local")
    val sc = new SparkContext(conf)
    val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numRDD = sc.parallelize(nums, 5)
    val sum = numRDD.reduce(_ + _)
    println("The sum:" + sum)
  }
}
