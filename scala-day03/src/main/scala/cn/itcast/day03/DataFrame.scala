package cn.itcast.day03

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @author: Liyuxin wechat:13011800146.
  * @Title: DataFrame
  * @ProjectName spark-demo
  * @date: 2019/3/18 14:51
  * @description:
  */
case class Person(id:Int,name: String,age:Int)

object DataFrame {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("DataFrameTest").master("local[2]").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val data: RDD[String] = sc.textFile("D:\\001 JavaWeb\\00 itheima\\04 就业班\\day67-spark03\\资料\\person.txt")

    val line: RDD[Array[String]] = data.map(x=>x.split(" "))

    val personRDD: RDD[Person] = line.map(x=>Person(x(0).toInt,x(1),x(2).toInt))

    import spark.implicits._
    val df: DataFrame = personRDD.toDF()
    // DSL start
    df.show()
    df.select("name").show()
    // DSL end

    // SQL start
    df.show()
    df.createOrReplaceTempView("person")
    spark.sql("select age from person").show()

    // SQL end


    //DataFrame与DataSet的相互转换
    val ds: Dataset[Person] = df.as[Person]
    ds.toDF()
    ds.show()

    sc.stop()
    spark.stop()


  }
}
