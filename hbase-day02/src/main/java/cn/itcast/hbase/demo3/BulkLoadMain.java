package cn.itcast.hbase.demo3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: Liyuxin wechat:13011800146. @Title: MyMapper @ProjectName spark-demo
 * @date: 2019/3/23 16:12
 * @description:
 */
public class BulkLoadMain extends Configured implements Tool {
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = super.getConf();

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("myuser2"));


		Job job = Job.getInstance(conf, "bulkLoad");
		job.setInputFormatClass(TextInputFormat.class);
		TextInputFormat.addInputPath(job,new Path("hdfs://node001:8020/hbase/input"));

		job.setMapperClass(BulkLoadMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		//直接将数据进行输出
		/**
		 * Job job,
		 * Table table,
		 * RegionLocator regionLocator
		 * 通过configureIncrementalLoad 方法指定我们的数据需要加载到哪个表的哪个region里面去
		 */
		HFileOutputFormat2.configureIncrementalLoad(job,table,connection.getRegionLocator(TableName.valueOf("myuser2")));


		//设置我们的输出文件的类相关为HFiel格式
		job.setOutputFormatClass(HFileOutputFormat2.class);
		HFileOutputFormat2.setOutputPath(job,new Path("hdfs://node001:8020/hbase/output_hfile4"));

		boolean b = job.waitForCompletion(true);

		return b?0:1;
	}

	public static void main(String[] args) throws Exception {
		//
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum","node001:2181,node002:2181,node003:2181");
		int run = ToolRunner.run(configuration, new BulkLoadMain(), args);
		System.exit(run);
	}
}
