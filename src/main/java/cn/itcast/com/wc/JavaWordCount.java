package cn.itcast.com.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author: Liyuxin wechat:13011800146. @Title: JavaWordCount @ProjectName spark-demo
 * @date: 2019/3/16 23:45
 * @description:
 */
public class JavaWordCount {
  public static void main(String[] args) {
    // 1.初始化对象
    SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[2]");

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    sc.setLogLevel("WARN");

    // 2.加载文件
    JavaRDD<String> data =
        sc.textFile(
            "D:\\001 JavaWeb\\00 itheima\\04 就业班\\day65-spark01\\课后\\spark_day01\\day65-spark01 代码\\hello.txt");

    // 3.切分并压平
    JavaRDD<String> words =
        data.flatMap(
            new FlatMapFunction<String, String>() {

              @Override
              public Iterator<String> call(String s) throws Exception {
                String[] words = s.split(" ");
                return Arrays.asList(words).iterator();
              }
            });

    // 4.将单词组成元组(单词,1)
    JavaPairRDD<String, Integer> wordAndNum =
        words.mapToPair(
            new PairFunction<String, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
              }
            });

    // 5.统计汇总
    JavaPairRDD<String, Integer> result =
        wordAndNum.reduceByKey(
            new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
              }
            });

    // 6.打印结果
    List<Tuple2<String, Integer>> collect = result.collect();

    for (Tuple2<String, Integer> tuple2 : collect) {
      System.out.println(tuple2) ;
    }

    // 7.释放资源
    sc.stop();
  }
}
