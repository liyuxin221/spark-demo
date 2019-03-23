package cn.itcast.hbase.demo1;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: MyReducer @ProjectName spark-demo
 * @date: 2019/3/23 14:43
 * @description:
 */
public class MyReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {
  @Override
  protected void reduce(Text key, Iterable<Put> values, Context context)
      throws IOException, InterruptedException {
    for (Put value : values) {
    	context.write(null,value);
	}
  }
}
