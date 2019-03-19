package cn.itcast.day03

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import java.util.Properties
/**
  */
object ReadAndWriteDataFromMysql {
  def main(args: Array[String]): Unit = {
    //初始化操作
    val spark: SparkSession = SparkSession.builder().appName("DataFrameTest").master("local[2]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //设置数据库连接的用户名和密码
    val properties: Properties = new Properties
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")

    //从数据库中读取数据
    val dataFrame: DataFrame = spark.read.jdbc("jdbc:mysql://node003:3306/spark", "iplocation", properties)
    dataFrame.show()
    dataFrame.printSchema()

    //将数据写入到数据库当中  :数据 ---->写入 ---->数据库当中
    dataFrame.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://node003:3306/spark", "iplocation999", properties)

    dataFrame.cache()

    //资源释放
    sc.stop()
    spark.stop()


  }
}
