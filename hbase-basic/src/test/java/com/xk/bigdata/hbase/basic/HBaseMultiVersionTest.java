package com.xk.bigdata.hbase.basic;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseMultiVersionTest {

    final String zookeeperQuorum = "bigdatatest02";

    @Before
    public void setUp() throws Exception {
        HBaseMultiVersion.init(zookeeperQuorum);
    }

    @After
    public void cleanUp() throws Exception {
        HBaseMultiVersion.close();
    }

    @Test
    public void testGetAllVersionRecord() throws IOException {
        HBaseMultiVersion.getAllVersionRecord("test:dem1", -1);
    }

}