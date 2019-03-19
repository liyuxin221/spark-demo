package cn.itcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 需求:访问次数最多用户前五个
  * */
object TOPN {
  def main(args: Array[String]): Unit = {
    //初始化
    val sparkConf: SparkConf = new SparkConf().setAppName("TOPN").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //获取数据
    val data: RDD[String] = sc.textFile("E:\\Java\\dataResource\\access.log.20181101.dat")

    //获取ip
    val ips: RDD[String] = data.map(_.split(" ")(0))

    //ip=>(ip,1)
    val ipsTuple: RDD[(String, Int)] = ips.map(x=>(x,1))
    //ip计数
    val ipAndCount: RDD[(String, Int)] = ipsTuple.reduceByKey(_ + _)
    //按照次数降序排列
    val ipSort: RDD[(String, Int)] = ipAndCount.sortBy(_._2,false)

    val buffer: mutable.Buffer[(String, Int)] = ipSort.collect().toBuffer
    println(buffer)

    for (x<-0 to 4){
      println(buffer(x))
    }
  }
}
