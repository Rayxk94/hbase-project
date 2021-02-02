package com.xk.bigdata.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtils {

    public static Connection connection;

    /**
     * 创建连接
     *
     * @param zookeeperQuorum ： Zookeeper 连接地址
     * @throws Exception
     */
    public static void init(String zookeeperQuorum) throws Exception {
        Configuration hbaseConf = new Configuration();
        hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum);
        Configuration conf = HBaseConfiguration.create(hbaseConf);
        connection = ConnectionFactory.createConnection(conf);
    }

    /**
     * 关闭连接
     *
     * @throws Exception
     */
    public static void close() throws Exception {
        if (!connection.isClosed()) {
            connection.close();
        }
    }

    /**
     * 创建表
     *
     * @param tableName ：表名
     * @param familys   ：列簇数组
     * @throws IOException
     */
    public static void createTable(String tableName, String[] familys) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + "已经存在");
        } else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String family : familys) {
                tableDescriptor.addFamily(new HColumnDescriptor(family));
            }
            admin.createTable(tableDescriptor);
            System.out.println(tableName + "创建成功！！");
        }
    }

    /**
     * 插入数据
     *
     * @param tableName ： 表名
     * @param rowKey    ： 主键
     * @param family    ： 列簇
     * @param qualifier ： 字段名
     * @param value     ： 字段数据
     * @throws IOException
     */
    public static void putRecord(String tableName, String rowKey, String family, String qualifier, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        table.put(put);
        System.out.println(tableName + "中字段：" + qualifier + "插入成功！！");
    }

    /**
     * 得到 hbase 中的一条数据
     *
     * @param tableName ：表名
     * @param rowKey    ： 主键
     * @throws IOException
     */
    public static void getOneRecord(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                    + " : " + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
                    + ":" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                    + " : " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
        }
    }

    /**
     * 得到表中的所有数据
     *
     * @param tableName
     * @throws IOException
     */
    public static void getAllRecord(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
                        + " : " + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
                        + ":" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
                        + " : " + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
        }
    }

    /**
     * 删除数据
     *
     * @param tableName ：表名
     * @param rowKey    ：主键
     * @throws IOException
     */
    public static void deleteRecord(String tableName, String rowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(tableName));
        table.delete(delete);
        System.out.println(tableName + "=====》" + rowKey + "删除成功！！！");
    }
}
