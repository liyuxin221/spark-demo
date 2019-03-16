package cn.itcast.com.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求:通过spark程序进行单词计数
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    //创建sparkConf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount")

    //创建sparkContext对象
    val sc = new SparkContext(sparkConf)

    //设置日志级别
    sc.setLogLevel("WARN")

    //单词计数

    //1.加载文件
    val data: RDD[String] = sc.textFile(args(0))


    //2.切分压平
    val words: RDD[String] = data.flatMap(_.split(" "))
    //3.将出现的单词组成元组 x->(x,1)
    val wordsTuple: RDD[(String, Int)] = words.map(x=>(x,1))
    //4.根据相同单词出现的次数进行累加
    val result2: RDD[(String, Int)] = wordsTuple.reduceByKey(_+_)
    //对结构进行排序
    val result: RDD[(String, Int)] = result2.sortBy(_._2)
    //5.收集数据 将计算的结果输出到hdfs
//    val finalResult: Array[(String, Int)] = result.collect()


//    finalResult.foreach(x=>println(x._1+"\t"+x._2))

    result.saveAsTextFile(args(0))
    //释放资源
    sc.stop()


  }

}
