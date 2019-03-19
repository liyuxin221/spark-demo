package cn.itcast.com.day02

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  */
object RDDTest2 {
  def main(args: Array[String]): Unit = {
    //初始化
    val sparkConf: SparkConf = new SparkConf().setAppName("RDDTest1" + UUID.randomUUID()).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,5,6,7,8,9))

    val rddMu110: RDD[Int] = rdd1.map(x=>x*10)

    val result: Array[Int] = rddMu110.collect()

    val filter: RDD[Int] = rdd1.filter(x=>x>=5)

    println(filter.collect().toBuffer)
  }
}
