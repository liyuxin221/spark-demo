package cn.itcast.hbase.demo1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author: Liyuxin wechat:13011800146. @Title: HBaseMain @ProjectName spark-demo
 * @date: 2019/3/23 14:43
 * @description:
 */
public class HBaseMain extends Configured implements Tool {
  public static void main(String[] args) throws Exception {

    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "node001:2181,node002:2181,node003:2181");
    int run = ToolRunner.run(configuration, new HBaseMain(), args);

    System.exit(run);
  }

  @Override
  public int run(String[] strings) throws Exception {
    Job job = Job.getInstance(this.getConf(), "HBaseJob");

    Scan scan = new Scan();

    // 使用工具类初始化tableMapper
    TableMapReduceUtil.initTableMapperJob(
        "myuser", scan, MyMapper.class, Text.class, Put.class, job);

    TableMapReduceUtil.initTableReducerJob("myuser2", MyReducer.class, job);

    job.setNumReduceTasks(1);
    boolean b = job.waitForCompletion(true);

    return b ? 0 : 1;
  }
}
