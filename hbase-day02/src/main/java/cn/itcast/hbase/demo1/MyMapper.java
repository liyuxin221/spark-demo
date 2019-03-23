package cn.itcast.hbase.demo1;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author: Liyuxin wechat:13011800146. @Title: MyMapper @ProjectName spark-demo
 * @date: 2019/3/23 11:48
 * @description:
 */
public class MyMapper extends TableMapper<Text, Put> {

  @Override
  protected void map(ImmutableBytesWritable key, Result value, Context context)
      throws IOException, InterruptedException {
    // 获取rowKey的字节数组
    byte[] bytes = key.get();
    String rowKey = new String(bytes);
    Put put = new Put(bytes);
    // f1 列族的数据 id name age 都封装在哪里了
    Cell[] cells = value.rawCells();

    for (Cell cell : cells) {
      // 需要判断哪些cell是我们f1 列族下面 id name age字段
      // 获取列族名称
      byte[] familyBytes = CellUtil.cloneFamily(cell);
      if ("f1".equals(new String(familyBytes))) {
        // 继续判断我们只取id name列
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        if (("id".equals(new String(qualifier))) || "name".equals(new String(qualifier))) {
          put.add(cell);
        }
      }
    }
    if (!put.isEmpty()) {
      context.write(new Text(rowKey), put);
    }
  }
}
