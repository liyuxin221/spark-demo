package cn.itcast.hbase.demo3;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: BulkLoadMapper @ProjectName spark-demo
 * @date: 2019/3/23 16:12
 * @description:
 */
public class BulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] split = value.toString().split("\t");
    Put put = new Put(split[0].getBytes());

    put.addColumn("f1".getBytes(), "name".getBytes(), split[1].getBytes());
    put.addColumn("f1".getBytes(), "age".getBytes(), split[2].getBytes());

    context.write(new ImmutableBytesWritable(split[0].getBytes()),put);
  }
}
