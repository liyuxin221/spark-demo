package cn.itcast

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  */
object ipLocation {
  def main(args: Array[String]): Unit = {
    //数据文件夹 D:\001 JavaWeb\00 itheima\04 就业班\day66-spark02\spark_day02\data
    //登录地址信息
    //20090121000132.394251.http.format
    //ip--经纬度信息
    //ip.txt

    //1.初始化
    val sparkConf: SparkConf = new SparkConf().setAppName("TOPN").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //2.加载字典数据(起始ip,终止ip,经度,维度) ip段-->经纬度信息
    val ips: RDD[String] = sc.textFile("D:\\001 JavaWeb\\00 itheima\\04 就业班\\day66-spark02\\spark_day02\\data\\ip.txt")

    //3.获取数据(起始ip,终止ip,经度,维度)
    val map: RDD[(String, String, String, String)] = ips.map(_.split("\\|")).map(x => (x(2), x(3), x(13), x(14)))

    //4.将获取到的数据进行收集
    val collect: Array[(String, String, String, String)] = map.collect()

    //5.广播字典数据到进程当中(只需要广播自己的数据就OK,不需要其他的东西,也不广播RDD)
    val broadcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(collect)

    //6.加载业务数据
    val ipInfos: RDD[String] = sc.textFile("D:\\001 JavaWeb\\00 itheima\\04 就业班\\day66-spark02\\spark_day02\\data\\20090121000132.394251.http.format")

    //7.分割数据
    //8.获取业务数据中的ip地址
    val destData: RDD[String] = ipInfos.map(x => x.split("\\|")(1))
    val ipAddr: RDD[String] = destData.distinct()


    //对于分区中每一个元素进行操作
    val partitions: RDD[((String, String), Int)] = ipAddr.mapPartitions(
      inter => {
        //9.获取广播变量的值
        val valueArr: Array[(String, String, String, String)] = broadcast.value

        //10.对分区中的每一个元素进行操作
        inter.map(ip => {

          //11.将ip地址转换为long类型
          val ipNum: Long = ipToLong(ip)

          //12.二分查找
          val index: Int = binarySearch(ipNum, valueArr)

          val arr: (String, String, String, String) = valueArr(index)

          ((arr._3, arr._4), 1)
        })
      })

    val key: RDD[((String, String), Int)] = partitions.reduceByKey(_ + _)

    //15.打印输出结果
    key.foreach(x=> println(x))

    //16.将((经度,维度),1)数据 写入数据库
    key.map(x=>(x._1._1,x._1._2,x._2)).foreachPartition(data2Mysql)

    //17.释放资源
    sc.stop()


  }


  /**
    * 将ip地址转换为long类型
    *
    * @param ip 192.168.120.250
    * @return
    */
  def ipToLong(ip: String): Long = {
    val split: Array[String] = ip.split("\\.")
    var ipNum = 0L

    for (i <- split) {
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分查找算法
    *
    * @param ip       ip为long类型的数据
    * @param valueArr ：字典数据（起始ip，终止ip，经度，纬度）
    * @return ：当前字典数据数组的下表
    */
  def binarySearch(ip: Long, valueArr: Array[(String, String, String, String)]): Int = {

    var start = 0
    var end = valueArr.length - 1

    while (start <= end) {
      var middle = (start + end) / 2

      if (ip >= valueArr(middle)._1.toLong && ip <= valueArr(middle)._2.toLong) {
        return middle
      }

      if (ip < valueArr(middle)._1.toLong) {
        end = middle
      }

      if (ip > valueArr(middle)._2.toLong) {
        start = middle
      }
    }
    -1
  }


  /**
    * 将数据写入到mysql当中
    *
    * @param iterator
    */
  def data2Mysql(iterator: Iterator[(String, String, Int)]): Unit = {

    //链接对象
    var conn: Connection = null

    var ps: PreparedStatement = null

    var sql = "insert into iplocation(longitude,latitude,total_count) values(?,?,?)"

    //获取链接对象
    conn = DriverManager.getConnection("jdbc:mysql://node003:3306/spark", "root", "root")

    try {
      iterator.foreach(line => {

        ps = conn.prepareStatement(sql)

        //第一个占位符 值为：经度
        ps.setString(1, line._1)
        ps.setString(2, line._2)
        ps.setLong(3, line._3)

        ps.execute()
      })
    } catch {
      case e: Exception => println(e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
}

