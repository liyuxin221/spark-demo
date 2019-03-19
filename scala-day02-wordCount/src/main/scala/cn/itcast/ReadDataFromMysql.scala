package cn.itcast

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: Liyuxin wechat:13011800146.
  * @Title: ReadDataFromMysql
  * @ProjectName spark-demo
  * @date: 2019/3/19 20:46
  * @description:
  */
object ReadDataFromMysql {
  def main(args: Array[String]): Unit = {
    //初始化
    val sparkConf: SparkConf = new SparkConf().setAppName("TOPN").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
  }
}
