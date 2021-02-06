package com.xk.bigdata.hbase.observer;

import com.xk.bigdata.hbase.utils.ESUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class HBaseElasticsearchObserver extends BaseRegionObserver {

    private static final Log LOG = LogFactory.getLog(HBaseElasticsearchObserver.class);

    /**
     * 读取配置文件
     *
     * @param env 上下文
     */
    private static void readConfiguration(CoprocessorEnvironment env) {
        Configuration conf = env.getConfiguration();
        ESUtils.clusterName = conf.get("es_cluster");
        ESUtils.nodeHost = conf.get("es_host");
        ESUtils.nodePort = conf.getInt("es_port", -1);
        ESUtils.indexName = conf.get("es_index");
    }

    /**
     * 开始之前执行
     */
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        // 读取配置文件
        readConfiguration(e);
        // 初始化 ES
        ESUtils.iniClient();
        LOG.error("------observer init EsClient ------" + ESUtils.getInfo());
    }

    /**
     * 最后执行
     */
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        ESUtils.stopClient();
    }

    /**
     * 插入数据
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        String indexId = new String(put.getRow());
        try {
            NavigableMap<byte[], List<Cell>> familyMap = put.getFamilyCellMap();
            Map<String, Object> map = new HashMap<String, Object>();
            for (Map.Entry<byte[], List<Cell>> entry : familyMap.entrySet()) {
                for (Cell cell : entry.getValue()) {
                    String key = Bytes.toString(CellUtil.cloneQualifier(cell));
                    String value = Bytes.toString(CellUtil.cloneValue(cell));
                    map.put(key, value);
                }
            }
            IndexRequest indexRequest = new IndexRequest(ESUtils.indexName).id(indexId);
            indexRequest.source(map);
            IndexResponse indexResponse =
                    ESUtils.client.index(indexRequest, RequestOptions.DEFAULT);

            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("create success:" + ESUtils.indexName + " : " + indexId);

            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {

                System.out.println("update success: " + ESUtils.indexName + " : " + indexId);

            } else {

                System.out.println("create/update fail: " + ESUtils.indexName + " : " + indexId + " : " + indexResponse.getResult().toString());
            }
        } catch (Exception ex) {
            LOG.error(ex);
            LOG.error("observer put  a doc, index [ " + ESUtils.indexName + " ]" + "indexId [" + indexId + "] error : " + ex.getMessage());
        }
    }

    /**
     * 删除数据
     */
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        String indexId = new String(delete.getRow());
        try {
            DeleteRequest request = new DeleteRequest(
                    ESUtils.indexName,
                    indexId);

            DeleteResponse deleteResponse = ESUtils.client.delete(
                    request, RequestOptions.DEFAULT);
            if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                System.out.println("delete success: " + ESUtils.indexName + " : " + indexId);
            } else {
                System.out.println("delete fail: " + ESUtils.indexName + " : " + indexId + " : " + deleteResponse.getResult().toString());
            }
        } catch (Exception ex) {
            LOG.error(ex);
            LOG.error("observer delete  a doc, index [ " + ESUtils.indexName + " ]" + "indexId [" + indexId + "] error : " + ex.getMessage());
        }
    }
}
