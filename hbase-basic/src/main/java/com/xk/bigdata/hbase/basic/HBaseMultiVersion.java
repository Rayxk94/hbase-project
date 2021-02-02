package com.xk.bigdata.hbase.basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseMultiVersion {

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
     * 得到固定版本的整条数据
     *
     * @param tableName ： 表名
     * @param version   ： 版本号
     * @throws IOException
     */
    public static void getAllVersionRecord(String tableName, Integer version) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.setMaxVersions(version);
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

}