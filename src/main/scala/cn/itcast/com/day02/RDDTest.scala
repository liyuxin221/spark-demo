package cn.itcast.com.day02

import java.util.UUID

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 创建RDD的三种方式
  * 1.通过已经存在的结合进行创建
  *   应用场景:字典数据|26个字母,省份
  * 2.通过外部的存储介质来创建RDD
  *   应用场景:hdfs,mysql,s3
  * 3.通过已经存在的RDD转换生成新的RDD
  *
  * map&maapPartitions区别
  *   map:作用于每一个元素
  *   mapPartitions:作用于每个分区
  *
  *   应用场景:对于元素的转换操作,用Map即可,对于数据要写入到mysql当中,需要连接数据库.
  *   如果是10个元素,map操作10个连接,而mapPartitions是N个连接(N=分区个数)
  **/
object RDDTest {
  def main(args: Array[String]): Unit = {
    //初始化
    val sparkConf: SparkConf = new SparkConf().setAppName("RDDTest1" + UUID.randomUUID())
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //创建RDD:第一种方式
    val RDD1: RDD[Int] = sc.parallelize(Array(1,2,3,4,5,6))

    //通过外部的存储介质来创建RDD
    val RDD2: RDD[String] = sc.textFile("hdfs://node001:8020/spark-data/hello.txt")

    //通过已经存在的RDD转换生成新的RDD
    val RDD3: RDD[String] = RDD2.flatMap(_.split(" "))

//    RDD3.mapPartitions()



  }

}
