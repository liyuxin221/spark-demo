package cn.itcast.day03

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
  * @author: Liyuxin wechat:13011800146.
  * @Title: StructTypeDemo
  * @ProjectName spark-demo
  * @date: 2019/3/18 15:07
  * @description:
  */

object StructTypeDemo {
  def main(args: Array[String]): Unit = {

    //初始化信息
    val spark: SparkSession = SparkSession.builder().appName("StructTypeDemo").master("local[2]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    //通过spark.createDataFrame来创建DataFrame

    val data: RDD[String] = sc.textFile("D:\\001 JavaWeb\\00 itheima\\04 就业班\\day65-spark02\\spark_day02\\data.txt")

    val lineArr: RDD[Array[String]] = data.map(x => x.split(" "))
    //构建row对象
    val rowRDD: RDD[Row] = lineArr.map(x => Row(x(0).toInt, x(1), x(2).toInt))
    //构建StructType对象
    val schema = StructType(
      StructField("id", IntegerType, false) ::
        StructField("name", StringType, false) ::
        StructField("age", IntegerType, false) :: Nil
    )

//    通过row以及structType来得到DF
    val dataFrame: DataFrame = spark.createDataFrame(rowRDD, schema)


    import spark.implicits._

    println(dataFrame)
    dataFrame.show()


  }
}
