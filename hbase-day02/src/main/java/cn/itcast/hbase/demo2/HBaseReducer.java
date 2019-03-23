package cn.itcast.hbase.demo2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: HBaseReducer @ProjectName spark-demo
 * @date: 2019/3/23 15:42
 * @description:
 */
public class HBaseReducer extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {
  @Override
  protected void reduce(Text key, Iterable<NullWritable> values, Context context)
      throws IOException, InterruptedException {
    // key里面封装了一行数据
    String[] split = key.toString().split("\t");

    // 输出的数据还是封装到Put对象里面去
    Put put = new Put(split[0].getBytes());
    put.addColumn("f1".getBytes(), "name".getBytes(), split[1].getBytes());
    put.addColumn("f1".getBytes(), "age".getBytes(), split[2].getBytes());

    context.write(new ImmutableBytesWritable(split[0].getBytes()),put);
  }
}
