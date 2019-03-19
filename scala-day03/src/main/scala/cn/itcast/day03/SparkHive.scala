package cn.itcast.day03

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object SparkHive {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().appName("SparkHive").master("local[2]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","scala-day03/spark-warehouse")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

//    spark.sql("create table if not exists student2(id int,name String,age int) row format delimited fields terminated by ','")
//    spark.sql("load data local inpath 'scala-day03/data' overwrite into table student2 ")
    spark.sql("select * from student2").show()

    sc.stop()
    spark.stop()


  }
}
