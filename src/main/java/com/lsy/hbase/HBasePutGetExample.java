package com.lsy.hbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by lisiyu on 2016/12/20.
 * HBase的Put和Get的例子
 */
public class HBasePutGetExample {

    void putExample() throws IOException {
        // 创建put
        Put put = new Put(Bytes.toBytes("rowkey"));

        // 添加字段
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("col"), Bytes.toBytes("value"));
        long putTimeStamp = 10000L;
        put.add(Bytes.toBytes("cf"), Bytes.toBytes("col"), putTimeStamp, Bytes.toBytes("value"));
        KeyValue keyValue = new KeyValue(Bytes.toBytes("rowkey"), Bytes.toBytes("cf"), Bytes.toBytes("col"), putTimeStamp, KeyValue.Type.Put, Bytes.toBytes("value"));
        put.add(keyValue);

        // 获取键值对
        List<Cell> cellList = put.get(Bytes.toBytes("cf"), Bytes.toBytes("col"));

        // 判断是否存在
        boolean isExist = put.has(Bytes.toBytes("cf"), Bytes.toBytes("col"));
        isExist = put.has(Bytes.toBytes("cf"), Bytes.toBytes("col"), Bytes.toBytes("value"));
        isExist = put.has(Bytes.toBytes("cf"), Bytes.toBytes("col"), putTimeStamp, Bytes.toBytes("value"));
    }
}
