package cn.itcast.com.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author: Liyuxin wechat:13011800146.
  * @Title: ReduceTest
  * @ProjectName spark-demo
  * @date: 2019/3/18 17:52
  * @description:
  */
object ReduceTest {
  def main(args: Array[String]): Unit = {
    //创建sparkconf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    //创建sparkContext对象
    val sc: SparkContext = new SparkContext(sparkConf)
    //设置日志级别
    sc.setLogLevel("WARN")

    val rdd1=sc.parallelize(List(("Tom",1),("jerry",3),("kitty",2)))

    val rdd2=sc.parallelize(List(("jerry",2),("Tom",3),("kitty",2)))

    println("group by")

    val rdd3: RDD[(String, Int)] = rdd1.union(rdd2)
    println(rdd3.groupByKey().collect().toBuffer)

    println("cogroup")
    val rdd4=rdd1.cogroup(rdd2)
    println(rdd4.collect().toBuffer)

    println("reduceByKey,sortByKey")
    //按照key进行聚合
    val rdd5: RDD[(String, Int)] = rdd3.reduceByKey((x,y)=>x+y)
    println(rdd5.collect().toBuffer)

    val rdd6: RDD[(String, Int)] = rdd5.sortBy(_._2,false)
    println(rdd6.collect().toBuffer)

    val rdd11=sc.parallelize(1 to 10,3)

    println(rdd11.repartition(2).partitions.size)

    println(rdd11.repartition(4).partitions.size)

    println(rdd11.coalesce(2).partitions.size)

  }

}
