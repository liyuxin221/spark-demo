package cn.itcast.hbase.demo2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 读取文件,将数据送到map阶段来进行处理
 * 第一个参数:输入的key的类型,其实就是行偏移量
 * 第二个参数:输入的的value的类型,其实就是我们一行文本内容
 * 第三个参数:输出的k2的类型,装我们一行文本数据
 * 第四个参数:输出的v2的类型,
 */
public class HdfsMapper extends Mapper<LongWritable, Text,Text,NullWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.write(value, NullWritable.get());
	}
}
