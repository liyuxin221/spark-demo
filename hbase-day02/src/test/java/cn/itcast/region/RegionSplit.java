package cn.itcast.region;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.testng.annotations.Test;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: RegionSplit @ProjectName spark-demo
 * @date: 2019/3/23 21:57
 * @description:
 */
public class RegionSplit {

  /** 通过javaAPI进行HBase的表的创建以及预分区操作 */
  @Test
  public void hbaseSplit() throws IOException {
    // 获取连接
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "node001:2181,node002:2181,node003:2181");
    Connection connection = ConnectionFactory.createConnection(configuration);
    Admin admin = connection.getAdmin();
    // 自定义算法，产生一系列Hash散列值存储在二维数组中
    byte[][] splitKeys = {{1, 2, 3, 4, 5}, {'a', 'b', 'c', 'd', 'e'}};

    // 通过HTableDescriptor来实现我们表的参数设置，包括表名，列族等等
    HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("stuff5"));
    // 添加列族
    hTableDescriptor.addFamily(new HColumnDescriptor("f1"));
    // 添加列族
    hTableDescriptor.addFamily(new HColumnDescriptor("f2"));
    admin.createTable(hTableDescriptor, splitKeys);
    admin.close();
  }
}
