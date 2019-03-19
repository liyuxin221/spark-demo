package cn.itcast.com.day02

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
  * spark缓存机制：
  *
  *   1、缓存什么数据？
  *     答：一般情况下，缓存shuffle之后的rdd
  *
  *   2、缓存代码实现是如何的？
  *     答：    //缓存的第一种方式
  *            result.cache()
  *             //缓存的第二种方式
  *           result.persist()
  *           result.persist(StorageLevel.MEMORY_ONLY)
  *
  *     注意事项：
  *       1、result.cache()&result.persist()--------->Persist this RDD with the default storage level (`MEMORY_ONLY`)
  *       2、result.persist(StorageLevel.MEMORY_ONLY)--->有多种存储级别可以选择
  *
  *   3、什么时候触发缓存?
  *     答：触发了action算子之后，才会执行缓存
  *
  *
  *
  *   4、在任务调度过程中，在调度的哪个阶段发生缓存
  *
  *     答：在任务调度过程当中，在DAGSchedual调度阶段发生该操作
  *
  *
  *
  *   chkpoint机制：
  *   1、代码实现过程是如何的？必须要设置chkpoint的目录
  *          //设置chkpoint的目录
  *          sc.setCheckpointDir("hdfs://hadoop-01:9000/spark-chkpoint")
  *           result.checkpoint()
  *
  *   总结：cache &persist&chkpoint之间的区别：
  *     1、cache &persist执行完成之后，血统关系不会变化
  *     2、chkpoint之后，血统关系就没有了，血缘关系断了
  *     3、如果设置了cache，persist，chkpoint，读取这些数据的时候，顺序是怎么样的？
  *     读取顺序：首先在内存当中，cache中读取数据，如果没有读取到，就在chkpoint中读取数据，如果在该阶段在chkpoint中还没有读取到数据
  *     由于已经设置了chkpoint，没有依赖关系了，只能从头在来
  *
  *
  *     cache  ----> chkpoint ---RDD重新计算
  *
  *
  *
  *
  */
object Cache2 {
  def main(args: Array[String]): Unit = {


    //创建sparkconf对象
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    //创建sparkContext对象
    val sc: SparkContext = new SparkContext(sparkConf)
    //设置日志级别
    sc.setLogLevel("WARN")

    //设置chkpoint的目录
    sc.setCheckpointDir("hdfs://hadoop-01:9000/spark-chkpoint")


    //1、加载文件
    val data: RDD[String] = sc.textFile("D:\\aa.txt")

    //2、先切分再压平
    val words: RDD[String] = data.flatMap(_.split(" "))
    //3、将出现的单词组成元组（单词，1）
    //words.map((_,1))
    val wordAnd1: RDD[(String, Int)] = words.map(x =>(x,1))
    //4、根据相同单词出现的次数进行累加
    val result: RDD[(String, Int)] = wordAnd1.reduceByKey(_+_)

    //缓存的第一种方式
    result.cache()
  //缓存的第二种方式
    result.persist()
    result.persist(StorageLevel.MEMORY_ONLY_SER)


    result.checkpoint()




    result.collect()

    sc.stop()
  }
}
