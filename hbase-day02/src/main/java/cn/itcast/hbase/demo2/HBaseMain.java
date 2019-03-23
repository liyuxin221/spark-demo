package cn.itcast.hbase.demo2;

import cn.itcast.hbase.demo1.MyReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: Liyuxin wechat:13011800146. @Title: HBaseMain @ProjectName spark-demo
 * @date: 2019/3/23 15:48
 * @description:
 */
public class HBaseMain extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "node001:2181,node002:2181,node003:2181");
    int run = ToolRunner.run(configuration, new cn.itcast.hbase.demo1.HBaseMain(), args);

    System.exit(run);
  }

  @Override
  public int run(String[] strings) throws Exception {
    Job job = Job.getInstance(this.getConf(), "HBJob");

    // 读取文件,解析成为k-v对
    job.setInputFormatClass(TextInputFormat.class);
    TextInputFormat.addInputPath(job, new Path("hdfs://node001:8020/hbase/input"));

    // 第二步:自定义map逻辑,接收k1,v1转换成k2,v2
    job.setMapperClass(HdfsMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    // 第三步:分区
    // 第四步:排序
    // 第五步:规约
    // 第六步:分组 全部省略

    // 第七步 自定义reducer
	  TableMapReduceUtil.initTableReducerJob("myuser2",HBaseReducer.class,job);

    job.setNumReduceTasks(2);

	  boolean b = job.waitForCompletion(true);


	  return b?0:1;
  }
}
