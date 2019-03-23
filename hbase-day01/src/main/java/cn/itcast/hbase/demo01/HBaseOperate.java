package cn.itcast.hbase.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: Liyuxin wechat:13011800146. @Title: HBaseOperate @ProjectName spark-demo
 * @date: 2019/3/21 16:41
 * @description:
 */
public class HBaseOperate {
  Configuration configuration = null;
  Connection connection = null;
  Table myuser = null;
  Admin admin = null;

  @BeforeTest
  public void init() throws IOException {
    // 设置配置项里面连接的参数
    configuration = HBaseConfiguration.create();
    // 指定zk连接地址就可以创建表
    configuration.set("hbase.zookeeper.quorum", "node001:2181,node002:2181,node003:2181");

    // 先连接hbase
    connection = ConnectionFactory.createConnection(configuration);
    myuser = connection.getTable(TableName.valueOf("myuser"));
    // 获取管理hbase的管理对象admin
    admin = connection.getAdmin();
  }

  @AfterTest
  public void close() throws IOException {

    // 关闭连接
    admin.close();
    connection.close();
  }

  @Test
  public void createTable() throws IOException {

    HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("myuser"));

    descriptor.addFamily(new HColumnDescriptor("f1"));
    descriptor.addFamily(new HColumnDescriptor("f2"));

    admin.createTable(descriptor);
  }

  // 添加数据
  @Test
  public void putData() throws IOException {
    Put put1 = new Put("0001".getBytes());

    put1.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
    put1.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("张三"));
    put1.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));

    put1.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("地球人"));
    put1.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15874102589"));

    myuser.put(put1);
  }

  @Test
  public void insertBatchData() throws IOException {

    connection = ConnectionFactory.createConnection(configuration);
    // 获取表
    Table myuser = connection.getTable(TableName.valueOf("myuser"));
    // 创建put对象，并指定rowkey
    Put put = new Put("0002".getBytes());
    put.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(1));
    put.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("曹操"));
    put.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(30));
    put.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
    put.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("沛国谯县"));
    put.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("16888888888"));
    put.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("helloworld"));

    Put put2 = new Put("0003".getBytes());
    put2.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(2));
    put2.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("刘备"));
    put2.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(32));
    put2.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
    put2.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("幽州涿郡涿县"));
    put2.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("17888888888"));
    put2.addColumn(
        "f2".getBytes(), "say".getBytes(), Bytes.toBytes("talk is cheap , show me the code"));

    Put put3 = new Put("0004".getBytes());
    put3.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(3));
    put3.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("孙权"));
    put3.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(35));
    put3.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
    put3.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("下邳"));
    put3.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("12888888888"));
    put3.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("what are you 弄啥嘞！"));

    Put put4 = new Put("0005".getBytes());
    put4.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(4));
    put4.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("诸葛亮"));
    put4.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
    put4.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
    put4.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("四川隆中"));
    put4.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("14888888888"));
    put4.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("出师表你背了嘛"));

    Put put5 = new Put("0005".getBytes());
    put5.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
    put5.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("司马懿"));
    put5.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(27));
    put5.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
    put5.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("哪里人有待考究"));
    put5.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15888888888"));
    put5.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("跟诸葛亮死掐"));

    Put put6 = new Put("0006".getBytes());
    put6.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(5));
    put6.addColumn("f1".getBytes(), "name".getBytes(), Bytes.toBytes("xiaobubu—吕布"));
    put6.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(28));
    put6.addColumn("f2".getBytes(), "sex".getBytes(), Bytes.toBytes("1"));
    put6.addColumn("f2".getBytes(), "address".getBytes(), Bytes.toBytes("内蒙人"));
    put6.addColumn("f2".getBytes(), "phone".getBytes(), Bytes.toBytes("15788888888"));
    put6.addColumn("f2".getBytes(), "say".getBytes(), Bytes.toBytes("貂蝉去哪了"));

    List<Put> listPut = new ArrayList<Put>();
    listPut.add(put);
    listPut.add(put2);
    listPut.add(put3);
    listPut.add(put4);
    listPut.add(put5);
    listPut.add(put6);

    myuser.put(listPut);
    myuser.close();
  }

  /*
   * 按照rowKey进行查询
   * */
  @Test
  public void getByRowKey() throws IOException {
    Get get1 = new Get(Bytes.toBytes("0003"));

    // result里面封装了查询的数据
    Result result1 = myuser.get(get1);

    Cell[] cells = result1.rawCells();

    for (Cell cell : cells) {
      byte[] value = cell.getValue();

      // 获取我们的数据在哪一个列
      byte[] bytes = CellUtil.cloneQualifier(cell);
      System.out.println(new String(bytes));
      System.out.println("---------------");

      if (new String(bytes).equals("age")) {
        // 通过Bytes工具类实现我们数据转int
        System.out.println(Bytes.toInt(value));
      }

      // 注意 如果使用new String 的方式打印数据,只能打印字符串的数据

      System.out.println(new String(value));
    }
  }
  /*
   * 查询指定的列族
   * */
  @Test
  public void getWithRowFamily() throws IOException {
    Get get = new Get("0003".getBytes());

    // 限制只查询某一个列族下面的数据
    get.addFamily("f1".getBytes());

    Result result = myuser.get(get);

    Cell[] cells = result.rawCells();

    for (Cell cell : cells) {
      byte[] value = cell.getValue();

      // 获取我们的数据在哪一个列
      byte[] bytes = CellUtil.cloneQualifier(cell);
      System.out.println("当前列:" + new String(bytes));

      if (new String(bytes).equals("age")) {
        // 通过Bytes工具类实现我们数据转int
        System.out.println(Bytes.toInt(value));
      }
    }
  }

  /** 指定startRow以及endRow进行范围扫描 */
  @Test
  public void scanRowKey() throws IOException {
    Scan scan = new Scan();

    // 扫描是包前不包后,按照rowKey的字典顺序
    //    scan.setStartRow("0003".getBytes());
    //    scan.setStopRow("0006".getBytes());

    ResultScanner scanner = myuser.getScanner(scan);

    scanResult(scanner);
  }

  /** 查询所有比003小的rowKey */
  @Test
  public void rowFilterTest() throws IOException {
    Scan scan = new Scan();
    scan.setFilter(
        new RowFilter(
            CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator("0003".getBytes())));

    ResultScanner scanner = myuser.getScanner(scan);

    scanResult(scanner);
  }

  /** 列族过滤器,在服务器端就给你过滤 查询f2列族的数据 */
  @Test
  public void familyFilterTest() throws IOException {
    Scan scan = new Scan();
    FamilyFilter familyFilter =
        new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("f2".getBytes()));
    scan.setFilter(familyFilter);

    ResultScanner scanner = myuser.getScanner(scan);

    scanResult(scanner);
  }

  /** 列过滤器，只查名为name的数据 */
  @Test
  public void qualifierFilter() throws IOException {
    QualifierFilter filter =
        new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("name".getBytes()));
    Scan scan = new Scan();
    scan.setFilter(filter);

    ResultScanner scanner = myuser.getScanner(scan);

    scanResult(scanner);
  }

  /** 查询列值当中包含8的数据 */
  @Test
  public void valueFilter() throws IOException {
    ValueFilter filter =
        new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("8"));

    Scan scan = new Scan();
    scan.setFilter(filter);

    ResultScanner scanner = myuser.getScanner(scan);

    scanResult(scanner);
  }

  /** 使用singleColumnValueFilter--专用过滤器能够返回所有字段(行的其他列也能够返回.) 查询名字是刘备的人 */
  @Test
  public void getLiuBei() throws IOException {
    Scan scan = new Scan();

    scan.setFilter(
        new SingleColumnValueFilter(
            "f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes()));
    ResultScanner results = myuser.getScanner(scan);

    scanResult(results);
  }

  /** 前缀过滤器 */
  @Test
  public void prefixFilterStudy() throws IOException {
    Scan scan = new Scan();

    PrefixFilter filter = new PrefixFilter("0001".getBytes());

    scan.setFilter(filter);

    ResultScanner scanner = myuser.getScanner(scan);

    scanResult(scanner);
  }

  /** hbase分页操作 需要自己封装 */
  @Test
  public void pageOperate() throws IOException {
    int pageNum = 2;
    int pageSize = 3;

    Integer starRow = (pageNum - 1) * pageSize + 1;
    Integer endRow = pageNum * pageSize + 1;

    //    pageNum = 1;
    if (pageNum == 1) {
      Scan scan = new Scan();

      PageFilter filter = new PageFilter(pageSize);
      scan.setFilter(filter);

      // 第一条数据的rowKey从哪里开始扫描
      scan.setStartRow("".getBytes());
      scan.setMaxResultSize(pageSize);

      ResultScanner scanner = myuser.getScanner(scan);

      scanResult(scanner);

    } else {

      String rowKey = "";
      for (int i = pageNum - 1; i <= pageNum; i++) {
        if (i == pageNum) {
          Scan scan = new Scan();
          scan.setStartRow(rowKey.getBytes());
          scan.setMaxResultSize(pageSize);

          PageFilter filter = new PageFilter(pageSize);
          scan.setFilter(filter);

          ResultScanner scanner = myuser.getScanner(scan);

          scanResult(scanner);

        } else {
          // pageNum!=1 我们需要计算起始的rowKey

          Scan scan = new Scan();
          scan.setStartRow(rowKey.getBytes());

          PageFilter filter = new PageFilter((pageNum - 1) * pageSize + 1);
          scan.setFilter(filter);
          scan.setMaxResultSize(pageSize);

          ResultScanner scanner1 = myuser.getScanner(scan);
          for (Result result : scanner1) {
            rowKey = new String(result.getRow());
            // System.out.println(rowKey);
          }
        }
      }
    }
  }

  /** hbase分页test */
  @Test
  public void pageOperate2() throws IOException {
    int pageNum = 1;
    int pageSize = 2;

    Integer starRow = (pageNum - 1) * pageSize + 1;
    Integer endRow = pageNum * pageSize + 1;

    String rowKey = "";


        Scan scan = new Scan();
        scan.setStartRow(rowKey.getBytes());
        scan.setMaxResultSize(pageSize);

        PageFilter filter = new PageFilter(pageSize);
        scan.setFilter(filter);

        ResultScanner scanner = myuser.getScanner(scan);

        scanResult(scanner);

    }


  /** 多过滤器综合查询 */
  @Test
  public void filterListSearch() throws IOException {

    Scan scan = new Scan();
    FilterList filterList = new FilterList();
    // 单列过滤器
    SingleColumnValueFilter singleColumnValueFilter =
        new SingleColumnValueFilter(
            "f1".getBytes(), "name".getBytes(), CompareFilter.CompareOp.EQUAL, "刘备".getBytes());

    // 前值过滤器
    PrefixFilter prefixFilter = new PrefixFilter("00".getBytes());

    // 添加过滤器
    filterList.addFilter(singleColumnValueFilter);
    filterList.addFilter(prefixFilter);

    scan.setFilter(filterList);
    ResultScanner scanner = myuser.getScanner(scan);

    scanResult(scanner);
  }

  /**
   * 工具方法:输入scanner,查询并打印结果
   *
   * @param scanner
   */
  private void scanResult(ResultScanner scanner) {
    for (Result result : scanner) {
      // 获取数据的rowKey
      byte[] row = result.getRow();
      System.out.println("当前rowKey:" + "\t" + new String(row));

      // 限制只查询某一个列族下面的数据
      // get.addFamily("f1".getBytes());

      Cell[] cells = result.rawCells();

      for (Cell cell : cells) {
        byte[] value = cell.getValue();

        // 获取我们的数据在哪一个列
        byte[] bytes = CellUtil.cloneQualifier(cell);
        System.out.println("当前列:" + "\t" + new String(bytes));

        String current = new String(bytes);

        if (current.equals("age") || current.equals("id")) {
          // 通过Bytes工具类实现我们数据转int
          System.out.println(Bytes.toInt(value));
          continue;
        }
        System.out.println(new String(value));
      }
      System.out.println("-------------");
    }
  }

  /*删除操作*/
  /** 根据rowKey删除数据 */
  @Test
  public void deleteData() throws IOException {

    myuser.delete(new Delete("0006".getBytes()));
  }

  /** 删除表操作 */
  @Test
  public void deleteTable() throws IOException {
    Admin admin = connection.getAdmin();
    admin.disableTable(TableName.valueOf("myuser"));
    admin.deleteTable(TableName.valueOf("myuser"));

    admin.close();
  }
}
