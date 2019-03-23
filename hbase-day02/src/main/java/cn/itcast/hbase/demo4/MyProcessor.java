package cn.itcast.hbase.demo4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;



public class MyProcessor extends BaseRegionObserver {

	/**
	 *
	 * @param e 上下文对象
	 * @param put 我们插入proc1表里面的数据,全部封装在了Put里面
	 * @param edit
	 * @param durability
	 * @throws IOException
	 */
  @Override
  public void prePut(
      ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
      throws IOException {
    // 获取连接
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "node001,node002,node003");
    Connection connection = ConnectionFactory.createConnection(configuration);
    Cell nameCell = put.get("info".getBytes(), "name".getBytes()).get(0);
    Put put1 = new Put(put.getRow());
    put1.add(nameCell);
    Table reverseuser = connection.getTable(TableName.valueOf("proc2"));
    reverseuser.put(put1);
    reverseuser.close();
  }
}
