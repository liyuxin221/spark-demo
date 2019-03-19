package cn.itcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: Liyuxin wechat:13011800146.
  * @Title: PV
  * @ProjectName spark-demo
  * @date: 2019/3/18 18:25
  * @description:
  */
object PV {


  def main(args: Array[String]): Unit = {

    //初始化
    val sparkConf: SparkConf = new SparkConf().setAppName("PV").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //读取数据
    val data: RDD[String] = sc.textFile("E:\\Java\\dataResource\\access.log.20181101.dat")

    //将一行数据作为输入,输出
    val result: Long = data.count()

    println(result)

    sc.stop()

  }
}
