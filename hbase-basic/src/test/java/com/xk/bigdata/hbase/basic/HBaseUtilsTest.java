package com.xk.bigdata.hbase.basic;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseUtilsTest {

    final String zookeeperQuorum = "bigdatatest02";

    @Before
    public void setUp() throws Exception {
        HBaseUtils.init(zookeeperQuorum);
    }

    @After
    public void cleanUp() throws Exception {
        HBaseUtils.close();
    }

    @Test
    public void testCreateTable() throws Exception {
        HBaseUtils.createTable("test:demo1", new String[]{"o"});
    }

    @Test
    public void testPutRecord() throws Exception {
        HBaseUtils.putRecord("test:dem1", "row2", "o", "id", "3");
    }

    @Test
    public void testGetOneRecord() throws IOException {
        HBaseUtils.getOneRecord("test:demo1", "row1");
    }

    @Test
    public void testGetAllRecord() throws IOException {
        HBaseUtils.getAllRecord("test:demo1");
    }

    @Test
    public void testDeleteRecord() throws IOException {
        HBaseUtils.deleteRecord("test:demo1", "row2");
    }
}
