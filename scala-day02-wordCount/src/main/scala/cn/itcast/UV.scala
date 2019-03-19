package cn.itcast

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求:计算UV
  *
  * */
object UV {
  def main(args: Array[String]): Unit = {
    //初始化
    val sparkConf: SparkConf = new SparkConf().setAppName("UV").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //加载文件
    val data: RDD[String] = sc.textFile("E:\\Java\\dataResource\\access.log.20181101.dat")

    //切分数据,获取用户ip
    val f:String=>String=(x:String)=>{
      val infos: Array[String] = x.split(" ")
      infos(0)
    }
    val ips: RDD[String] = data.map(f)

    val distinctRDD: RDD[String] = ips.distinct()

    val result: Long = distinctRDD.count()

    //打印结果
    println(distinctRDD.collect().toBuffer)
    println(result)

    //释放资源
    sc.stop()
  }
}
